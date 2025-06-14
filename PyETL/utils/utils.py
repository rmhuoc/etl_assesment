# src/utils.py
import random
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
import random
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import yaml
from sqlalchemy import create_engine, text,inspect, Column, Table, MetaData, String, text
import logging
from sqlalchemy.exc import SQLAlchemyError
import traceback
import re

def create_mock_data(config):
    """
    Generate mock data based on the provided configuration.

    This function reads mock data settings from the configuration and creates CSV files
    with fake but structurally consistent data for testing or development.

    Supported column types:
    - int_sequence: Generates a sequential integer column starting at 1.
    - random_int_<start>_<end>: Random integer in the given range.
    - datetime_now_minus_random_minutes_0_100000: Random datetime within the last ~70 days.
    - random_unique_int_start_end: Ensures unique integers across rows within the range.

    Parameters:
    config (dict): Configuration dictionary containing:
        - file_path: Path to save the generated CSV.
        - num_rows: Number of rows to generate.
        - columns: Dictionary defining column names and their generation rules.

    Returns:
    None
    """
    for dataset in config.get("mock_data", []):
        path = dataset["file_path"]
        num_rows = dataset.get("num_rows", 1000)
        columns = dataset["columns"]

        data = []
        base_time = datetime.now()

        # Si la columna es random_unique_int_1_9999999, generamos lista única
        unique_ids = None
        for col_type in columns.values():
            if col_type.startswith("random_unique_int_"):
                start, end = map(int, col_type[len("random_unique_int_"):].split("_"))
                unique_ids = random.sample(range(start, end + 1), num_rows)

        for i in range(num_rows):
            row = {}
            for col_name, col_type in columns.items():
                if col_type == "int_sequence":
                    row[col_name] = i + 1
                elif col_type == "random_int_1000_1100":
                    row[col_name] = random.randint(1000, 1100)
                elif col_type == "random_int_200_250":
                    row[col_name] = random.randint(200, 250)
                elif col_type == "random_int_1_10":
                    row[col_name] = random.randint(1, 10)
                elif col_type == "datetime_now_minus_random_minutes_0_100000":
                    row[col_name] = base_time - timedelta(minutes=random.randint(0, 100000))
                elif col_type.startswith("random_unique_int_"):
                    # Asignamos ID único de la lista
                    row[col_name] = unique_ids[i]
                else:
                    raise ValueError(f"Unsupported column type: {col_type}")
            data.append(row)

        df = pd.DataFrame(data)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False, encoding='utf-8')
        logging.info(f"Generated mock data saved to {path} with {num_rows} rows.")


def setup_logging(config):
    """
    Configure the global logging settings for the ETL process.
    """
    log_dir = config.get('logging', {}).get('log_dir', 'logs')
    log_file = config.get('logging', {}).get('log_file', 'etl.log')
    when = config.get('logging', {}).get('when', 'midnight')
    interval = config.get('logging', {}).get('interval', 1)
    backup_count = config.get('logging', {}).get('backup_count', 7)
    encoding = config.get('logging', {}).get('encoding', 'utf-8')

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, log_file),
        when=when,
        interval=interval,
        backupCount=backup_count,
        encoding=encoding
    )
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    


def load_config(path='config/config.yaml'):
    """
    Load the YAML configuration file from the given path.

    This function reads and parses a YAML file and returns it as a dictionary.
    It performs error handling for file access and parsing issues.

    Parameters:
    path (str): Path to the configuration file. Default is 'config/config.yaml'.

    Returns:
    dict: Parsed configuration dictionary.

    Raises:
    FileNotFoundError: If the configuration file does not exist.
    yaml.YAMLError: If there is a YAML syntax error.
    Exception: For any other unexpected errors.
    """
    try:
        if not os.path.exists(path):
            logging.error(f"Config file not found: {path}")
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path, 'r') as f:
            config = yaml.safe_load(f)

        logging.info(f"Configuration loaded successfully from {path}")
        return config

    except yaml.YAMLError as e:
        logging.error(f"YAML parsing error in {path}: {e}")
        raise

    except Exception as e:
        logging.exception(f"Unexpected error loading configuration from {path}")
        raise


def check_table_tmp(df, engine, schema, table_name):
    """
    Check if a temporary table exists in the database and create it if it doesn't.

    This utility is used to ensure a working table exists before loading data.
    It uses the schema of the provided DataFrame to infer the structure of the new table.

    - If the table already exists, nothing happens.
    - If it does not exist, the table is created with the appropriate columns.
    - If 'process_id' column is missing, it is added with integer dtype.

    Parameters:
    df (pd.DataFrame): DataFrame used to infer table schema.
    engine (sqlalchemy.Engine): SQLAlchemy engine for database connection.
    schema (str): Target schema in the database.
    table_name (str): Name of the table to check or create.

    Returns:
    None
    """
    inspector = inspect(engine)
    if not inspector.has_table(table_name, schema=schema):
        logging.info(f"Table {schema}.{table_name} doesn´t exist. Will be created.")
        if 'process_id' not in df.columns:
            df = df.copy()
            df['process_id'] = pd.Series(dtype='int')
            
        df.head(0).to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace',
            index=False
        )
        logging.info(f"Table {schema}.{table_name} created sucessfylly.")
    else:
        logging.info(f"Table {schema}.{table_name} already exists.")
        


def check_table_inc(engine, tmp_schema, tmp_table, target_schema, target_table):
    """
    Ensure the target table exists and is aligned with the structure of the temporary table.

    This function is used during incremental loads to:
    - Create the target table if it does not exist, using the structure of the temporary table.
    - Add any missing columns that exist in the temporary table but not in the target table.

    Parameters:
    engine (sqlalchemy.Engine): SQLAlchemy engine connected to the database.
    tmp_schema (str): Schema of the temporary table.
    tmp_table (str): Name of the temporary table.
    target_schema (str): Schema of the target table.
    target_table (str): Name of the target table.

    Returns:
    None
    """
    inspector = inspect(engine)

    if not inspector.has_table(target_table, schema=target_schema):
        logging.info(f"Target table {target_schema}.{target_table} doesn't exist. Creating it based on {tmp_schema}.{tmp_table}.")

        metadata = MetaData(schema=tmp_schema)
        tmp_tbl = Table(tmp_table, metadata, autoload_with=engine)

        # Create empty dataframe with columns from temp table
        columns = [col.name for col in tmp_tbl.columns]
        dtypes = {}
        for col in tmp_tbl.columns:
            if hasattr(col.type, "python_type"):
                dtypes[col.name] = col.type.python_type
            else:
                dtypes[col.name] = str

        df_empty = pd.DataFrame(columns=columns).astype(dtypes)

        # Add process_id column if not exists
        if "process_id" not in df_empty.columns:
            df_empty["process_id"] = pd.Series(dtype="int")

        df_empty.head(0).to_sql(
            name=target_table,
            con=engine,
            schema=target_schema,
            if_exists='replace',
            index=False
        )

        logging.info(f"Target table {target_schema}.{target_table} created based on schema of {tmp_schema}.{tmp_table}.")
    else:
        logging.info(f"Target table {target_schema}.{target_table} already exists. Checking for missing columns...")

        # Load columns from table
        tmp_metadata = MetaData(schema=tmp_schema)
        tmp_tbl = Table(tmp_table, tmp_metadata, autoload_with=engine)
        tmp_columns = {col.name for col in tmp_tbl.columns}

        tgt_metadata = MetaData(schema=target_schema)
        tgt_tbl = Table(target_table, tgt_metadata, autoload_with=engine)
        tgt_columns = {col.name for col in tgt_tbl.columns}

        missing_in_target = tmp_columns - tgt_columns

        for col in missing_in_target:
            try:
                alter_sql = f'ALTER TABLE "{target_schema}"."{target_table}" ADD COLUMN "{col}" TEXT'
                with engine.begin() as conn:
                    conn.execute(text(alter_sql))
                logging.info(f"Column '{col}' added to {target_schema}.{target_table} from {tmp_schema}.{tmp_table}.")
            except Exception as e:
                logging.error(f"Could not add column '{col}' to {target_schema}.{target_table}: {e}")
            
def infer_pg_type(series):
    """
    Infer the corresponding PostgreSQL data type from a pandas Series dtype.

    This function examines the data type of the given pandas Series and returns
    an appropriate PostgreSQL column type as a string. It covers common numeric,
    boolean, datetime, and fallback to text types.

    Parameters:
    series (pd.Series): A pandas Series whose dtype will be analyzed.

    Returns:
    str: A string representing the PostgreSQL data type inferred from the Series dtype.
         Possible return values include:
         - "BIGINT" for integer types,
         - "DOUBLE PRECISION" for float types,
         - "BOOLEAN" for boolean types,
         - "TIMESTAMP" for datetime types,
         - "TEXT" as a fallback for any other types.
    """
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    else:
        return "TEXT"

def sanitize_identifier(name):
    """
    Sanitize a string to be a valid SQL identifier by removing invalid characters.

    This function removes any character from the input string that is not
    an uppercase or lowercase letter, a digit, or an underscore. This ensures
    the resulting string can safely be used as a SQL identifier (e.g., table or column name).

    Parameters:
    name (str): The input string to sanitize.

    Returns:
    str: A sanitized string containing only letters, digits, and underscores.
    """
    return re.sub(r'[^a-zA-Z0-9_]', '', name)

def sync_dataframe_with_table_schema(df, engine, schema, table_name):
    """
    Synchronizes the columns of a pandas DataFrame with the schema of a PostgreSQL table.

    This function ensures that the DataFrame contains all columns present in the database table,
    adding missing columns to the DataFrame with None values. It also adds columns that exist in
    the DataFrame but are missing from the database table by issuing ALTER TABLE statements.

    Columns managed by the database, such as an auto-incrementing 'id' field, are ignored during synchronization.

    Parameters:
        df (pd.DataFrame): The DataFrame to synchronize.
        engine (sqlalchemy.Engine): SQLAlchemy engine connected to the PostgreSQL database.
        schema (str): Name of the schema in the database.
        table_name (str): Name of the table in the database.

    Returns:
        pd.DataFrame: The updated DataFrame, with columns added to match the database schema.
    """
    metadata = MetaData(schema=schema)
    # Reflect the target table metadata from the database
    table = Table(table_name, metadata, autoload_with=engine)
    db_columns = {col.name for col in table.columns}  # Columns existing in the DB table
    df_columns = set(df.columns)                      # Columns present in the DataFrame
    
    # Exclude DB-managed columns, e.g. auto-increment id
    ignored_columns = {'id'}
    db_columns -= ignored_columns

    # Add missing columns to the DataFrame with None values
    missing_in_df = db_columns - df_columns
    for col in missing_in_df:
        if col == "process_id":
            logging.info(f"Column '{col}' is missing in DataFrame. Skipping adding it with None values here.")
            continue
        df[col] = None
        logging.info(f"Column '{col}' was missing in DataFrame and was added with None values.")

    # Add missing columns to the database table based on DataFrame columns
    missing_in_db = df_columns - db_columns
    for col in missing_in_db:
        try:
            col_type = infer_pg_type(df[col])  # Infer PostgreSQL type from DataFrame column
            safe_col = sanitize_identifier(col)  # Sanitize column name for SQL
            alter_sql = f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{safe_col}" {col_type}'
            with engine.begin() as conn:
                conn.execute(text(alter_sql))  # Execute ALTER TABLE to add new column
            logging.info(f"Column '{col}' added to {schema}.{table_name} with type {col_type}.")
        except Exception as e:
            logging.error(f"Error adding column '{col}' to {schema}.{table_name}: {e}")
            logging.error(traceback.format_exc())

    return df




def align_types_df_to_db_schema(df, engine, schema, table_name):
    """
    Align the data types of a pandas DataFrame's columns to match the PostgreSQL table schema.

    This function inspects the PostgreSQL table's column types and attempts to cast
    the corresponding DataFrame columns to compatible pandas data types. Columns in the DataFrame
    that do not exist in the database schema are skipped with a warning.

    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will be type-aligned.
        engine (sqlalchemy.Engine): SQLAlchemy engine connected to the PostgreSQL database.
        schema (str): The schema name of the PostgreSQL database.
        table_name (str): The table name in the PostgreSQL database.

    Returns:
        pd.DataFrame: The DataFrame with columns cast to types aligned with the database schema.
    """
    metadata = MetaData(schema=schema)
    # Reflect the table metadata from the database
    table = Table(table_name, metadata, autoload_with=engine)

    # Get the set of column names from the DB table
    db_column_names = {col.name for col in table.columns}

    # Iterate over DataFrame columns to align types
    for col in df.columns:
        if col not in db_column_names:
            logging.warning(f"Column '{col}' not found in DB metadata. Skipping type alignment.")
            continue

        # Get the DB column type as string for matching
        db_type = table.columns[col].type
        try:
            db_type_str = str(db_type).upper()
            if 'INT' in db_type_str:
                # Cast to pandas nullable integer type
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif 'FLOAT' in db_type_str or 'DOUBLE' in db_type_str or 'NUMERIC' in db_type_str:
                # Cast to float
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
            elif 'DATE' in db_type_str or 'TIME' in db_type_str:
                # Cast to datetime
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif 'BOOL' in db_type_str:
                # Cast to boolean
                df[col] = df[col].astype(bool)
            else:
                # Default to string type
                df[col] = df[col].astype(str)
        except Exception as e:
            logging.warning(f"Could not cast column '{col}' to type {db_type}: {e}")

    return df


