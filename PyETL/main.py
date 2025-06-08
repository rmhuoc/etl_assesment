from utils.utils import generate_mock_data, setup_logging, load_config
from load.load import get_engine, load_data
from extract.extract import extract_csv 
from sqlalchemy import text  # Esto es necesario para ejecutar "SELECT 1"
import logging

def main():
    try:
        setup_logging()
        config = load_config()
        generate_mock_data(config)

        engine = get_engine(config['database'])

        # Verificación de conexión
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("Connection to database OK and verified.")
        except Exception as conn_err:
            logging.error(f"Database connection failed: {conn_err}")
            raise  

        for file_entry in config['files_to_tables']:
            file_path = file_entry['file_path']
            schema = file_entry['schema']
            table = file_entry['table']

            logging.info(f"Processing file {file_path} into {schema}.{table}")
            df = extract_csv(file_path)
            load_data(df, engine, table, schema=schema)
            logging.info(f"Loaded data from {file_path} into {schema}.{table}")

        logging.info("ETL process completed successfully.")



    except Exception as e:
        logging.error(f"ETL failed: {str(e)}")

if __name__ == "__main__":
    main()
