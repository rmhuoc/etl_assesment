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
from sqlalchemy import create_engine, text,inspect, Column, Table, MetaData, String
import logging
from sqlalchemy.exc import SQLAlchemyError

def generate_mock_data(config):
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


# def generate_mock_data(config):
    # for dataset in config.get("mock_data", []):
        # path = dataset["file_path"]
        # num_rows = dataset.get("num_rows", 1000)
        # columns = dataset["columns"]

        # data = []
        # base_time = datetime.now()

        # for i in range(num_rows):
            # row = {}
            # for col_name, col_type in columns.items():
                # if col_type == "int_sequence":
                    # row[col_name] = i + 1
                # elif col_type == "random_int_1000_1100":
                    # row[col_name] = random.randint(1000, 1100)
                # elif col_type == "random_int_200_250":
                    # row[col_name] = random.randint(200, 250)
                # elif col_type == "random_int_1_10":
                    # row[col_name] = random.randint(1, 10)
                # elif col_type == "random_int_1_9999999":
                    # row[col_name] = random.randint(1, 9_999_999)
                # elif col_type == "datetime_now_minus_random_minutes_0_100000":
                    # row[col_name] = base_time - timedelta(minutes=random.randint(0, 100000))
                # else:
                    # raise ValueError(f"Unsupported column type: {col_type}")
            # data.append(row)

        # df = pd.DataFrame(data)
        # os.makedirs(os.path.dirname(path), exist_ok=True)
        # df.to_csv(path, index=False, encoding='utf-8')
        # logging.info(f"Generated mock data saved to {path} with {num_rows} rows.")





def setup_logging():
    # Crear carpeta logs si no existe
    if not os.path.exists('logs'):
        os.makedirs('logs')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Eliminar handlers previos para evitar duplicados si la función se llama varias veces
    if logger.hasHandlers():
        logger.handlers.clear()

    handler = TimedRotatingFileHandler(
        filename='logs/etl.log',
        when='midnight',      # rota a medianoche
        interval=1,
        backupCount=7,        # guarda últimos 7 días de logs
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


def ensure_table_exists(df, engine, schema, table_name):
    """
    Crea la tabla si no existe en la base de datos usando el esquema del DataFrame.
    """
    inspector = inspect(engine)
    if not inspector.has_table(table_name, schema=schema):
        logging.info(f"La tabla {schema}.{table_name} no existe. Se va a crear.")
        df.head(0).to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace',
            index=False
        )
        logging.info(f"Tabla {schema}.{table_name} creada correctamente.")
    else:
        logging.info(f"La tabla {schema}.{table_name} ya existe.")

def add_missing_columns(df, engine, schema, table_name):
    """
    Añade columnas que estén en el DataFrame pero no en la tabla de destino.
    """
    metadata = MetaData(schema=schema)
    table = Table(table_name, metadata, autoload_with=engine)
    db_columns = {col.name for col in table.columns}
    df_columns = set(df.columns)

    missing_columns = df_columns - db_columns
    if not missing_columns:
        return

    logging.info(f"Columnas faltantes en {schema}.{table_name}: {missing_columns}")
    for col in missing_columns:
        try:
            alter_sql = f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{col}" TEXT'
            with engine.begin() as conn:
                conn.execute(alter_sql)
            logging.info(f"Columna '{col}' añadida a {schema}.{table_name}.")
        except SQLAlchemyError as e:
            logging.error(f"No se pudo añadir la columna '{col}': {e}")
