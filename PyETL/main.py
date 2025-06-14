from utils.utils import setup_logging, load_config, create_mock_data, check_table_inc
from utils.etl_monitor import start_etl_process, end_etl_process
from load.load import get_engine, validate_and_load_csv_file_in_chunks, incremental_insert
from extract.extract import extract_csv 
from sqlalchemy import text  
from datetime import datetime
from transform.transform import encrypt_dataframe, data_encryptation
import logging


def main():
    process_id = None
    total_loaded = 0
    error_message = None

    try:
        # Load configuration from YAML or other config file
        config = load_config() 
        
        # Setup logging based on config parameters (log level, file, format)
        setup_logging(config)   

        logging.info("Starting ETL process...")
        
        # Create DB engine using SQLAlchemy and connection params from config
        engine = get_engine(config['database'])  

        # Register ETL process in monitoring DB table, get unique process_id
        process_id = start_etl_process(engine, config)
        logging.info(f"ETL process started with process_id={process_id}")

        # Optionally generate mock data for testing or demonstration
        create_mock_data(config)
        
        # Encrypt CSV input files if required and update config paths accordingly
        for file_entry in config.get('files_to_tables_tmp', []):
            original_file = file_entry['file_path']
            encrypted_file = original_file.replace('.csv', '_encrypted.csv')
            data_encryptation(original_file, encrypted_file, config['encryption'])
            file_entry['file_path'] = encrypted_file

        # Quick check to verify DB connection is valid before loading
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Connection to database OK and verified.")

        # Read CSV files and load in chunks to avoid memory issues
        chunk_size = config.get("csv", {}).get("chunksize", 10000)

        for file_entry in config.get('files_to_tables_tmp', []):
            logging.info(f"Processing file {file_entry['file_path']} into {file_entry['schema']}.{file_entry['table']} using chunks of size {chunk_size}")
            validate_and_load_csv_file_in_chunks(
                file_path=file_entry['file_path'],
                engine=engine,
                schema=file_entry['schema'],
                table=file_entry['table'],
                process_id=process_id,
                chunk_size=chunk_size,
                config=config
            )

        # Perform incremental inserts from staging/temp tables into final target tables
        for inc_entry in config.get('files_to_tables_inc', []):
            check_table_inc(engine, inc_entry['tmp_schema'], inc_entry['tmp_table'], inc_entry['target_schema'], inc_entry['target_table'])
            logging.info(f"Performing incremental load from {inc_entry['tmp_schema']}.{inc_entry['tmp_table']} to {inc_entry['target_schema']}.{inc_entry['target_table']} for process_id {process_id}")
            inserted = incremental_insert(engine, inc_entry['tmp_schema'], inc_entry['tmp_table'], inc_entry['target_schema'], inc_entry['target_table'], inc_entry['unique_keys'], process_id)
            total_loaded += inserted

        logging.info(f"ETL process completed successfully process_id={process_id}. Total records loaded: {total_loaded}")

    except Exception as e:
        error_message = str(e)
        logging.error(f"ETL failed (process_id={process_id}): {error_message}")

    finally:
        if process_id is not None:
            # Mark ETL process completion and log summary in monitoring table
            end_etl_process(engine, config, process_id, total_loaded, error_message)



if __name__ == "__main__":
    main()
