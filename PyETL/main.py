from utils.utils import generate_mock_data, setup_logging, load_config, ensure_table_exists, add_missing_columns, ensure_table_exists_inc
from utils.etl_monitor import start_etl_process, end_etl_process
from load.load import get_engine, load_data, incremental_insert
from extract.extract import extract_csv 
from sqlalchemy import text  
from datetime import datetime
from transform.transform import encrypt_dataframe, encrypt_csv_to_new_file
import logging


def main():
    process_id = None
    total_loaded = 0
    error_message = None

    try:
        setup_logging()
        config = load_config()

        logging.info("Starting ETL process...")
        engine = get_engine(config['database'])

        # Start process and obtain process id
        process_id = start_etl_process(engine, config)
        logging.info(f"ETL process started with process_id={process_id}")

        # Generate mock data according config
        generate_mock_data(config)
        
        for file_entry in config.get('files_to_tables_tmp', []):
            original_file = file_entry['file_path']
            encrypted_file = original_file.replace('.csv', '_encrypted.csv')
            encrypt_csv_to_new_file(original_file, encrypted_file, config['encryption'])
            # Actualizamos el path para que luego usemos el CSV encriptado
            file_entry['file_path'] = encrypted_file

        # Verificate connection db
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Connection to database OK and verified.")

        # Process files into tmp created according config
        for file_entry in config.get('files_to_tables_tmp', []):
            file_path = file_entry['file_path']
            schema = file_entry['schema']
            table = file_entry['table']
            full_table_name = f"{schema}.{table}"
            
            df = extract_csv(file_path)
            #check if table exists, if not exists, is created
            ensure_table_exists(df, engine, schema, table)

            # Truncar tabla temporal antes de la carga
            with engine.connect() as conn:
                logging.info(f"Truncating temporary table {full_table_name}")
                conn.execute(text(f"TRUNCATE TABLE {full_table_name}"))

            logging.info(f"Processing file {file_path} into {full_table_name}")
            #check if extra columns in csv, if not in table, is added
            add_missing_columns(df, engine, schema, table)
            load_data(df, engine, table, schema=schema, process_id=process_id)

            loaded_count = len(df)
            total_loaded += loaded_count

            logging.info(f"Loaded {loaded_count} records from {file_path} into {full_table_name}")

        # Load from tmp to final
        for inc_entry in config.get('files_to_tables_inc', []):
            tmp_schema = inc_entry['tmp_schema']
            tmp_table = inc_entry['tmp_table']
            target_schema = inc_entry['target_schema']
            target_table = inc_entry['target_table']
            unique_keys = inc_entry['unique_keys']
            ensure_table_exists_inc(engine, tmp_schema,tmp_table,target_schema, target_table)

            logging.info(f"Performing incremental load from {tmp_schema}.{tmp_table} to {target_schema}.{target_table}")
            incremental_insert(engine, tmp_schema, tmp_table, target_schema, target_table, unique_keys)

        logging.info(f"ETL process completed successfully process_id={process_id}. Total records loaded: {total_loaded}")

    except Exception as e:
        error_message = str(e)
        logging.error(f"ETL failed (process_id={process_id}): {error_message}")

    finally:
        if process_id is not None:
            end_etl_process(engine, config, process_id, total_loaded, error_message)


if __name__ == "__main__":
    main()
