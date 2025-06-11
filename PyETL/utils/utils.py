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


def setup_logging():

    if not os.path.exists('logs'):
        os.makedirs('logs')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    handler = TimedRotatingFileHandler(
        filename='logs/etl.log',
        when='midnight',      # rotate at midnight
        interval=1,
        backupCount=7,        # onlye seven last days of log
        encoding='utf-8'
    )
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    


def load_config(path='config/config.yaml'):
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
        

from sqlalchemy import inspect, MetaData, Table
import pandas as pd
import logging

def check_table_inc(engine, tmp_schema, tmp_table, target_schema, target_table):
    """.
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

        # Cargar columnas de ambas tablas
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




# def add_missing_columns(df, engine, schema, table_name):
    # """
    
    # """
    # metadata = MetaData(schema=schema)
    # table = Table(table_name, metadata, autoload_with=engine)
    # db_columns = {col.name for col in table.columns}
    # df_columns = set(df.columns)
    
    # if 'process_id' not in db_columns:
       # logging.info(f"Adding mising column 'process_id' in {schema}.{table_name}")
       # try:
           # with engine.begin() as conn:
               # conn.execute(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "process_id" INTEGER')
           # logging.info(f"Column 'process_id' added sucessfully in {schema}.{table_name}.")
       # except SQLAlchemyError as e:
           # logging.error(f"Unable to add column 'process_id': {e}")

    # missing_columns = df_columns - db_columns
    # if not missing_columns:
        # return

    # logging.info(f"Missing columns in  {schema}.{table_name}: {missing_columns}")
    # for col in missing_columns:
        # try:
            # alter_sql = f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{col}" TEXT'
            # with engine.begin() as conn:
                # conn.execute(alter_sql)
            # logging.info(f"Column '{col}' added in {schema}.{table_name}.")
        # except SQLAlchemyError as e:
            # logging.error(f"Unable to add column '{col}': {e}")
            
def infer_pg_type(series):
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
    return re.sub(r'[^a-zA-Z0-9_]', '', name)

def sync_dataframe_with_table_schema(df, engine, schema, table_name):
    metadata = MetaData(schema=schema)
    table = Table(table_name, metadata, autoload_with=engine)
    db_columns = {col.name for col in table.columns}
    df_columns = set(df.columns)
    
    # Excluir columnas gestionadas por la BD (como ID autoincremental)
    ignored_columns = {'id'}
    db_columns -= ignored_columns


    # Añadir columnas faltantes al DataFrame
    missing_in_df = db_columns - df_columns
    for col in missing_in_df:
        df[col] = None
        logging.info(f"Column '{col}' missing in DataFrame. Added with None values.")

    # Añadir columnas faltantes a la base de datos
    missing_in_db = df_columns - db_columns
    for col in missing_in_db:
        try:
            col_type = infer_pg_type(df[col])
            safe_col = sanitize_identifier(col)
            alter_sql = f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{safe_col}" {col_type}'
            with engine.begin() as conn:
                conn.execute(text(alter_sql))
            logging.info(f"Column '{col}' added to {schema}.{table_name} with type {col_type}.")
        except Exception as e:
            logging.error(f"Error adding column '{col}' to {schema}.{table_name}: {e}")
            logging.error(traceback.format_exc())

    return df
