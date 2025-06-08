# src/load.py
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from concurrent.futures import ThreadPoolExecutor
import logging
import psycopg2

def get_engine(db_config):
    url = f"{db_config['dialect']}://{db_config['user']}:{db_config['password']}@" \
          f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(url)

def clean_strings(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: x.encode('utf-8', 'replace').decode('utf-8', 'replace') if isinstance(x, str) else x)
    return df
    
# def load_data(df, engine, table_name, schema=None, process_id=None):
    # try:
        # logging.info(f"Index name: {df.index.name}")
        # df.reset_index(inplace=True)
        # df = df.copy()
        # if 'index' in df.columns:
            # df.drop(columns=['index'], inplace=True)

        # df.to_sql(
            # name=table_name,
            # con=engine,
            # schema=schema,
            # if_exists='append',
            # index=False,
            # method='multi'
        # )
        # logging.info(f"Loaded {len(df)} records into {schema}.{table_name} (process_id={process_id})")
    # except psycopg2.errors.UniqueViolation as e:
        # logging.warning(f"Duplicate records detected (process_id={process_id}): {e}")
    # except psycopg2.errors.CheckViolation as e:
        # logging.warning(f"Constraint violation (e.g., quantity >= 1) detected (process_id={process_id}): {e}")
    # except Exception as e:
        # logging.error(f"ETL failed (process_id={process_id}): {e}")
        # raise
        
from sqlalchemy.exc import IntegrityError
import psycopg2

def load_data(df, engine, table_name, schema=None, process_id=None):
    try:
        logging.info(f"Index name: {df.index.name}")
        df.reset_index(inplace=True)
        df = df.copy()
        if 'index' in df.columns:
            df.drop(columns=['index'], inplace=True)

        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            method='multi'
        )

        logging.info(f"Loaded {len(df)} records into {schema}.{table_name} (process_id={process_id})")
    except IntegrityError as e:
        orig = getattr(e.orig, 'diag', None)
        if isinstance(e.orig, psycopg2.errors.UniqueViolation):
            detalle = orig.message_detail if orig and orig.message_detail else str(e)
            logging.warning(f"Duplicate records detected (process_id={process_id}): {detalle}")
        elif isinstance(e.orig, psycopg2.errors.CheckViolation):
            detalle = orig.message_detail if orig and orig.message_detail else str(e)
            logging.warning(f"Constraint violation (e.g., quantity >= 1) detected (process_id={process_id}): {detalle}")
        else:
            logging.error(f"Database integrity error (process_id={process_id}): {str(e)}")
    except Exception as e:
        logging.error(f"ETL failed (process_id={process_id}): {e}")
        raise
