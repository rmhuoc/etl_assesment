# src/utils.py
import random
from datetime import datetime, timedelta
import pandas as pd
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import yaml
from sqlalchemy import create_engine, text,inspect, Column, Table, MetaData, String, text
from sqlalchemy.exc import SQLAlchemyError
import traceback
import re
import shutil

def create_mock_data(config, process_id=None):
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
        - file_path: Path to save the generated CSV. Can include '{process_id}' placeholder.
        - num_rows: Number of rows to generate.
        - columns: Dictionary defining column names and their generation rules.
    process_id (int or str, optional): Process identifier to be included in file name.

    Returns:
    None
    """
    for dataset in config.get("mock_data", []):
        name, ext = os.path.splitext(dataset["file_path"])
        path = f"{name}_{process_id}{ext}"
        dataset["file_path"] = path  # opcional, para que el config se actualice si lo necesitas más adelante
   
        if process_id is not None:
            path = path.format(process_id=process_id)  # reemplaza el placeholder

        num_rows = dataset.get("num_rows", 1000)
        columns = dataset["columns"]

        data = []
        base_time = datetime.now()

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
    log_level_str = config.get('logging', {}).get('level', 'INFO').upper()
    
    # Convert to constant
    log_level = getattr(logging, log_level_str, logging.INFO)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger()
    logger.setLevel(log_level)

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
            logging.info(f"Column '{col}' is missing in DataFrame. Skipping adding it with null values here.")
            continue
        df[col] = ''
        logging.info(f"Column '{col}' was missing in DataFrame and was added with null values.")

    # Refresh db_columns set to include newly added ones (if done in prior executions)
    inspector = inspect(engine)
    existing_db_columns = set([col["name"] for col in inspector.get_columns(table_name, schema=schema)])

    # Add missing columns to the database table based on DataFrame columns
    missing_in_db = df_columns - existing_db_columns
    for col in missing_in_db:
        try:
            col_type = infer_pg_type(df[col])  # Infer PostgreSQL type from DataFrame column
            safe_col = sanitize_identifier(col)  # Sanitize column name for SQL
            alter_sql = f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{safe_col}" {col_type}'
            with engine.begin() as conn:
                conn.execute(text(alter_sql))  # Execute ALTER TABLE to add new column
            logging.info(f"Column '{col}' added to {schema}.{table_name} with type {col_type}.")
        except Exception as e:
            if 'already exists' in str(e):
                logging.warning(f"Column '{col}' already exists in {schema}.{table_name}. Skipping.")
            else:
                logging.error(f"Error adding column '{col}' to {schema}.{table_name}: {e}")
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(traceback.format_exc())

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


def archive_data_files(config, process_id):
    """
    Moves files from the data directory to an archive directory, filtering by process_id and 
    renaming them with a timestamp to avoid overwriting.

    Args:
        config (dict): Configuration dictionary with keys:
            - "paths": dict containing:
                - "data_dir" (str): Path to the data directory (default "data")
                - "archive_dir" (str): Path to the archive directory (default "data/archive")
        process_id (str or int): Identifier used to select files related to the current process.

    Logs each file moved with the new name.
    """
    data_dir = config.get("paths", {}).get("data_dir", "data")
    archive_dir = config.get("paths", {}).get("archive_dir", "data/archive")

    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)
        logging.info(f"Archive directory created at: {archive_dir}")

    for filename in os.listdir(data_dir):
        if str(process_id) not in filename:
            continue  # Skip files not related to this process

        file_path = os.path.join(data_dir, filename)
        if os.path.isfile(file_path):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name, ext = os.path.splitext(filename)
            new_filename = f"{name}_{timestamp}{ext}"
            dest_path = os.path.join(archive_dir, new_filename)

            shutil.move(file_path, dest_path)
            logging.info(f"Moved file {filename} to archive directory as {new_filename}")

def get_path_with_process_id(base_path: str, process_id: int) -> str:
    """
    Appends a process-specific identifier to a file path before the extension.

    This function is useful when running multiple instances of an ETL process concurrently,
    ensuring that each instance reads/writes to its own version of the file by embedding
    the process_id into the file name.

    Example:
        base_path = "data/sales_transactions.csv"
        process_id = 123
        Result → "data/sales_transactions_123.csv"

    Args:
        base_path (str): Original file path (e.g., "data/file.csv").
        process_id (int): Unique identifier for the current ETL process.

    Returns:
        str: Modified file path including the process ID.
    """
    #logging.info(f"base path{base_path}")
    name, ext = os.path.splitext(base_path)
    return f"{name}_{process_id}{ext}"


def assert_table_exists(engine, schema, table):
    """
    Verifies that a specified table exists in the given database schema.

    Uses SQLAlchemy's inspector to check if the table is present. 
    Raises a RuntimeError if the table does not exist.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine connected to the target database.
        schema (str): Name of the schema containing the table.
        table (str): Name of the table to check for existence.

    Raises:
        RuntimeError: If the specified table does not exist in the given schema.
    """
    inspector = inspect(engine)
    if not inspector.has_table(table, schema=schema):
        message = f"Required table {schema}.{table} does not exist."
        logging.error(message)
        raise RuntimeError(message)

