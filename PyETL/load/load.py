# src/load.py
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import psycopg2
from sqlalchemy import text  
from sqlalchemy.exc import IntegrityError
import traceback
from sqlalchemy.exc import SQLAlchemyError
from textwrap import dedent
import pandas as pd
from utils.utils import check_table_tmp, sync_dataframe_with_table_schema, align_types_df_to_db_schema
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_engine(db_config):
    """
    Create and return a SQLAlchemy engine based on the provided database configuration.

    Parameters:
        db_config (dict): Dictionary with keys 'dialect', 'user', 'password', 'host', 'port', 'database'.

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine instance connected to the database.
    """
    url = f"{db_config['dialect']}://{db_config['user']}:{db_config['password']}@" \
          f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(url)

def clean_strings(df):
    """
    Sanitize string columns in a DataFrame by encoding/decoding as UTF-8 to replace invalid characters.
    it´s deprecated,

    Parameters:
        df (pandas.DataFrame): Input DataFrame with potential string columns.

    Returns:
        pandas.DataFrame: DataFrame with cleaned string columns.
    """
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: x.encode('utf-8', 'replace').decode('utf-8', 'replace') if isinstance(x, str) else x)
    return df
    
def load_with_copy(df, engine, table_name, schema=None, process_id=None):
    """
    Load a pandas DataFrame into a PostgreSQL table using the COPY command for performance.

    Parameters:
        df (pandas.DataFrame): DataFrame to be inserted into the database.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for DB connection.
        table_name (str): Target table name.
        schema (str, optional): Database schema name.
        process_id (int, optional): Identifier for the current ETL process; added as a column.

    Behavior:
        - Resets DataFrame index.
        - Drops 'index' column if present.
        - Adds 'process_id' column if provided.
        - Loads data into the target table using PostgreSQL COPY FROM for performance.
        - Handles and logs common integrity errors.
    """
    import io
    import psycopg2
    from sqlalchemy import text

    try:
        #logging.info(f"Index name load: {df.index.name}")
        df.reset_index(drop=True, inplace=True)
        df = df.copy()

        if 'index' in df.columns:
            df.drop(columns=['index'], inplace=True)

        if process_id is not None:
            df['process_id'] = int(process_id)
            logging.info(f"Added process_id={process_id} to DataFrame before load.")

        # Convert DataFrame to CSV format in-memory
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # Build target table full name
        table_fullname = f'{schema}.{table_name}' if schema else table_name

        # Use raw connection for COPY
        raw_conn = engine.raw_connection()
        cursor = raw_conn.cursor()

        columns = ', '.join(df.columns)
        copy_sql = f"COPY {table_fullname} ({columns}) FROM STDIN WITH CSV"

        cursor.copy_expert(sql=copy_sql, file=buffer)
        raw_conn.commit()
        cursor.close()

        logging.info(f"Loaded {len(df)} records into {table_fullname} using COPY (process_id={process_id})")

    except psycopg2.IntegrityError as e:
        raw_conn.rollback()
        orig = getattr(e, 'diag', None)
        if isinstance(e, psycopg2.errors.UniqueViolation):
            detail = orig.message_detail if orig and orig.message_detail else str(e)
            logging.warning(f"Duplicate records detected (process_id={process_id}): {detail}")
        elif isinstance(e, psycopg2.errors.CheckViolation):
            detail = orig.message_detail if orig and orig.message_detail else str(e)
            logging.warning(f"Constraint violation (e.g., quantity >= 1) detected (process_id={process_id}): {detail}")
        else:
            logging.error(f"Database integrity error (process_id={process_id}): {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error (process_id={process_id}): {e.__class__.__name__} - {str(e)}")
        logging.debug(traceback.format_exc())
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'raw_conn' in locals():
            raw_conn.close()

        
def incremental_insert(engine, tmp_schema, tmp_table, target_schema, target_table, unique_keys, process_id):
    """
    Perform an incremental insert row-by-row from a temporary table to the target table,
    skipping rows that violate constraints and logging errors.

    Parameters:
        engine (sqlalchemy.engine.Engine): Database connection engine.
        tmp_schema (str): Schema name of the temporary staging table.
        tmp_table (str): Name of the temporary staging table.
        target_schema (str): Schema name of the target table.
        target_table (str): Name of the target table.
        unique_keys (list of str): List of column names representing the unique key.
        process_id (int): Current process ID to filter rows to insert.

    Returns:
        int: Number of rows inserted into the target table.
    """
    import pandas as pd
    inserted_rows = 0
    try:
        with engine.connect() as conn:
            query = f'SELECT * FROM "{tmp_schema}"."{tmp_table}" WHERE process_id = :pid'
            df = pd.read_sql_query(text(query), conn, params={"pid": process_id})

        if df.empty:
            logging.info("No rows found in temp table for this process_id.")
            return 0

        with engine.begin() as conn:
            for i, row in df.iterrows():
                try:
                    cols = row.index.tolist()
                    values = [row[col] for col in cols]
                    placeholders = ', '.join([f":{col}" for col in cols])
                    col_names = ', '.join([f'"{col}"' for col in cols])
                    insert_stmt = f'INSERT INTO "{target_schema}"."{target_table}" ({col_names}) VALUES ({placeholders})'
                    conn.execute(text(insert_stmt), row.to_dict())
                    inserted_rows += 1
                except Exception as row_e:
                    logging.warning(f"Failed to insert row {i}: {row.to_dict()} => {row_e}")

        logging.info(f"Inserted {inserted_rows} new records into {target_schema}.{target_table}")
        return inserted_rows

    except Exception as e:
        logging.error(f"Fatal error during incremental insert (process_id={process_id}): {e}")
        logging.error(traceback.format_exc())
        return 0
    
def validate_and_load_csv_file_in_chunks(file_path, engine, schema, table, process_id, chunk_size, config):
    """
    Reads a CSV file in chunks, applies validation rules to each chunk, and loads valid data into the database.

    Args:
        file_path (str): Path to the CSV file.
        engine (sqlalchemy.Engine): SQLAlchemy engine for database connection.
        schema (str): Target schema in the database.
        table (str): Target table in the database.
        process_id (int): Unique process ID to track this ETL execution.
        chunk_size (int): Number of rows per chunk.
        config (dict): Configuration dictionary containing validation rules, DB settings, and concurrency options.

    Returns:
        None
    """
    import pandas as pd
    import time
    import logging
    from concurrent.futures import ThreadPoolExecutor, as_completed

    total_loaded = 0

    logging.info(f"Reading file {file_path} in chunks of {chunk_size} with max_workers={config['csv']['max_workers']}")
    logging.info("=== ETL Configuration ===")
    logging.info(f"File path: {file_path}")
    logging.info(f"Chunk size: {chunk_size}")
    logging.info(f"Max workers: {config['csv'].get('max_workers', 1)}")
    logging.info(f"Target schema: {schema}")
    logging.info(f"Target table: {table}")
    logging.info("==========================")

    def process_and_load_chunk(chunk, idx):
        logging.info(f"[Chunk-{idx}] STARTED with {len(chunk)} rows")
        time.sleep(2)

        original_len = len(chunk)

        # Convert timestamps and filter out rows with future dates
        if 'timestamp' in chunk.columns:
            chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], utc=True, errors='coerce')

        max_ts_str = config.get('validation', {}).get('max_timestamp')
        max_timestamp = pd.to_datetime(max_ts_str, utc=True) if max_ts_str else pd.Timestamp.utcnow()
        chunk = chunk[chunk['timestamp'] <= max_timestamp]
        removed_future_dates = original_len - len(chunk)
        if removed_future_dates > 0:
            logging.info(f"[Chunk-{idx}] Removed {removed_future_dates} rows with timestamp in the future")

        # Filter based on 'quantity' range
        filters = config.get('tables', {}).get(table, {}).get('filters', {})
        quantity_filter = filters.get('quantity', {})
        min_qty = quantity_filter.get('min')
        max_qty = quantity_filter.get('max')
        before_qty = len(chunk)
        if min_qty is not None and max_qty is not None and 'quantity' in chunk.columns:
            invalid_rows = chunk[(chunk['quantity'] < min_qty) | (chunk['quantity'] > max_qty)]
            for _, row in invalid_rows.iterrows():
                logging.warning(f"[Chunk-{idx}] Dropped row due to quantity out of range: {row.to_dict()}")
            chunk = chunk[(chunk['quantity'] >= min_qty) & (chunk['quantity'] <= max_qty)]

        # Drop rows missing required columns
        required_columns = config.get('tables', {}).get(table, {}).get('required_columns', [])
        if required_columns:
            missing_required = chunk[chunk[required_columns].isnull().any(axis=1)]
            for _, row in missing_required.iterrows():
            
                # two alternative, we can remove row or we can replace null by n/a
                
                # logging.warning(f"[Chunk-{idx}] Dropped row missing required columns: {row.to_dict()}")
            # chunk = chunk.dropna(subset=required_columns)
                logging.warning(f"[Chunk-{idx}] Replaced missing required columns with 'n/a': {row.to_dict()}")
            chunk[required_columns] = chunk[required_columns].fillna('n/a')

        # Add process_id column
        chunk["process_id"] = pd.Series([process_id] * len(chunk), dtype="Int64")

        # Load chunk into the database using COPY
        load_with_copy(chunk, engine, table, schema=schema, process_id=process_id)

        logging.info(f"[Chunk-{idx}] FINISHED loading {len(chunk)} records")
        return len(chunk)

    reader = pd.read_csv(file_path, chunksize=chunk_size)
    try:
        first_chunk = next(reader)
    except StopIteration:
        logging.warning("CSV file is empty. No data to process.")
        return

    # Sync once: asegura compatibilidad y sincroniza esquema
    first_chunk = sync_dataframe_with_table_schema(first_chunk, engine, schema, table)
    first_chunk = align_types_df_to_db_schema(first_chunk, engine, schema, table)
    reference_columns = first_chunk.columns.tolist()

    with ThreadPoolExecutor(max_workers=config['csv']['max_workers']) as executor:
        futures = []
        futures.append(executor.submit(process_and_load_chunk, first_chunk, 0))

        for idx, chunk in enumerate(reader, start=1):
            chunk = chunk.reindex(columns=reference_columns)
            futures.append(executor.submit(process_and_load_chunk, chunk, idx))

        for future in as_completed(futures):
            total_loaded += future.result()

    logging.info(f"Finished loading file {file_path}. Total rows loaded: {total_loaded} (process_id={process_id})")
