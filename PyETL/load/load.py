# src/load.py
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from concurrent.futures import ThreadPoolExecutor
import logging
import psycopg2
from sqlalchemy import text  
from sqlalchemy.exc import IntegrityError
import traceback
from sqlalchemy.exc import SQLAlchemyError

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
        


def load_data(df, engine, table_name, schema=None, process_id=None):
    try:
        logging.info(f"Index name load: {df.index.name}")
        df.reset_index(drop=True,inplace=True)
        df = df.copy()
        if 'index' in df.columns:
            df.drop(columns=['index'], inplace=True)
            
        if process_id is not None:
            df['process_id'] = process_id
            logging.info(f"Added process_id={process_id} to DataFrame before load.")
       
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
            detail = orig.message_detail if orig and orig.message_detail else str(e)
            logging.warning(f"Duplicate records detected (process_id={process_id}): {detail}")
        elif isinstance(e.orig, psycopg2.errors.CheckViolation):
            detail = orig.message_detail if orig and orig.message_detail else str(e)
            logging.warning(f"Constraint violation (e.g., quantity >= 1) detected (process_id={process_id}): {detail}")
        else:
            logging.error(f"Database integrity error (process_id={process_id}): {str(e)}")
    except SQLAlchemyError as e:
        # Captura errores específicos de SQLAlchemy sin mostrar SQL completo
        logging.error(f"SQLAlchemy error (process_id={process_id}): {e.__class__.__name__} - {str(e).splitlines()[0]}")
        logging.debug(traceback.format_exc())  # Solo si necesitas el stack trace completo
        raise
    except Exception as e:
        logging.error(f"Unexpected error (process_id={process_id}): {e.__class__.__name__}")
        logging.debug(traceback.format_exc())  # Puedes quitar esto si no quieres ver nada
        raise
        
        
from sqlalchemy import text

def incremental_insert(engine, tmp_schema, tmp_table, target_schema, target_table, unique_keys):
    try:
        # Construye la condición de exclusión
        join_conditions = " AND ".join([
            f"t.{key} = s.{key}" for key in unique_keys
        ])

        with engine.begin() as conn:  
            # Obtain columns tmp table
            result = conn.execute(text(f"SELECT * FROM {tmp_schema}.{tmp_table} LIMIT 0"))
            columns = result.keys()
            column_list = ", ".join(columns)

            # Build SQL statement
            insert_sql = f"""
                INSERT INTO {target_schema}.{target_table} ({column_list})
                SELECT {column_list} FROM {tmp_schema}.{tmp_table} s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {target_schema}.{target_table} t
                    WHERE {join_conditions}
                )
            """

            #logging.info(f"SQL statement incremental insert:\n{insert_sql.strip()}")

            # Contar before
            count_tmp = conn.execute(text(f"SELECT COUNT(*) FROM {tmp_schema}.{tmp_table}")).scalar()
            #logging.info(f"{tmp_schema}.{tmp_table} contains {count_tmp} rows before incremental insert.")

            # Execute insert
            insert_result = conn.execute(text(insert_sql))
            logging.info(f"Incremental insert completed from {tmp_schema}.{tmp_table} to {target_schema}.{target_table}. Rows inserted: {insert_result.rowcount}")

            # Count after
            count_target = conn.execute(text(f"SELECT COUNT(*) FROM {target_schema}.{target_table}")).scalar()
            #logging.info(f"{target_schema}.{target_table} contains {count_target} rows after incremental insert.")

    except Exception as e:
        logging.error(f"Error during incremental insert from {tmp_schema}.{tmp_table} to {target_schema}.{target_table}: {e}")
        raise



