from utils.utils import generate_mock_data, setup_logging, load_config
from utils.etl_monitor import start_etl_process, end_etl_process
from load.load import get_engine, load_data
from extract.extract import extract_csv 
from sqlalchemy import text  
from datetime import datetime
from transform.transform import encrypt_dataframe
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

        # Verificate connection db
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Connection to database OK and verified.")

        # Process files created according config
        for file_entry in config['files_to_tables']:
            file_path = file_entry['file_path']
            schema = file_entry['schema']
            table = file_entry['table']

            logging.info(f"Processing file {file_path} into {schema}.{table}")
            df = extract_csv(file_path)
            logging.info(f"Index name: {df.index.name}")

            # Encrypt data
            df_encrypted = encrypt_dataframe(df, config['encryption'])
            logging.info(f"Index name: {df_encrypted.index.name}")
            load_data(df, engine, table, schema=schema)
            # Load data into db
            load_data(df_encrypted, engine, table, schema=schema, process_id=process_id)
            loaded_count = len(df)
            total_loaded += loaded_count

            logging.info(f"Loaded {loaded_count} records from {file_path} into {schema}.{table}")

        logging.info(f"ETL process completed successfully process_id={process_id}. Total records loaded: {total_loaded}")

    except Exception as e:
        error_message = str(e)
        logging.error(f"ETL failed (process_id={process_id}): {error_message}")

    finally:
        if process_id is not None:
            end_etl_process(engine, config, process_id, total_loaded, error_message)

if __name__ == "__main__":
    main()
