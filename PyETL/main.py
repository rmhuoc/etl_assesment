from utils.utils import setup_logging, load_config, create_mock_data, archive_data_files, get_path_with_process_id, sync_dataframe_with_table_schema, align_types_df_to_db_schema,assert_table_exists
from utils.etl_monitor import start_etl_process, end_etl_process
from load.load import get_engine, validate_and_load_csv_file_in_chunks, incremental_insert
from sqlalchemy import text, inspect
from datetime import datetime
from transform.transform import data_encryptation
import logging
import os
import pandas as pd



def main():
    process_id = None
    total_loaded = 0
    error_message = None

    try:
        config = load_config() 
        setup_logging(config)   
        logging.info("Starting ETL process...")

        engine = get_engine(config['database'])  
        process_id = start_etl_process(engine, config)
        logging.info(f"ETL process started with process_id={process_id}")

        create_mock_data(config, process_id)

        for file_entry in config.get('files_to_tables_tmp', []):
            base_file = file_entry['file_path']  
            original_file = get_path_with_process_id(base_file, process_id)  
            #logging.info(f"original file in call to encrypted  {original_file}")
            encrypted_file = original_file.replace('.csv', '_encrypted.csv')  

            data_encryptation(original_file, encrypted_file, config['encryption'])
            file_entry['file_path'] = encrypted_file

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Connection to database OK and verified.")

        chunk_size = config.get("csv", {}).get("chunk_size", 10000)

        for inc_entry in config.get('files_to_tables_inc', []):
            assert_table_exists(engine, inc_entry['target_schema'], inc_entry['target_table'])
            assert_table_exists(engine, inc_entry['tmp_schema'], inc_entry['tmp_table'])

        for file_entry in config.get('files_to_tables_tmp', []):
            file_path_with_pid = file_entry['file_path']
            logging.info(f"Processing file {file_path_with_pid} into {file_entry['schema']}.{file_entry['table']} using chunks of size {chunk_size}")
            validate_and_load_csv_file_in_chunks(
                file_path=file_path_with_pid,
                engine=engine,
                schema=file_entry['schema'],
                table=file_entry['table'],
                process_id=process_id,
                chunk_size=chunk_size,
                config=config
            )

        for inc_entry in config.get('files_to_tables_inc', []):
            logging.info(f"Performing alignment of temp table {inc_entry['tmp_schema']}.{inc_entry['tmp_table']} to match target table {inc_entry['target_schema']}.{inc_entry['target_table']}")

            with engine.connect() as conn:
                query = f"""
                    SELECT * FROM \"{inc_entry['tmp_schema']}\".\"{inc_entry['tmp_table']}\"
                    WHERE process_id = {process_id}
                """
                df_tmp = pd.read_sql(query, conn)
                #Drop 'id' from the DataFrame before syncing if it exists to avoid ALTER TABLE conflict
                if 'id' in df_tmp.columns:
                    df_tmp.drop(columns=['id'], inplace=True)
                    logging.info("Column 'id' dropped from temp DataFrame before schema sync.")

            
                df_tmp = sync_dataframe_with_table_schema(df_tmp, engine, inc_entry['target_schema'], inc_entry['target_table'])
          
                df_tmp = align_types_df_to_db_schema(df_tmp, engine, inc_entry['target_schema'], inc_entry['target_table'])
                

                logging.info(f"Aligned and replaced temp table {inc_entry['tmp_schema']}.{inc_entry['tmp_table']} before incremental insert.")

                logging.info(f"Performing incremental load from {inc_entry['tmp_schema']}.{inc_entry['tmp_table']} to {inc_entry['target_schema']}.{inc_entry['target_table']} for process_id {process_id}")
                inserted = incremental_insert(
                    engine,
                    inc_entry['tmp_schema'],
                    inc_entry['tmp_table'],
                    inc_entry['target_schema'],
                    inc_entry['target_table'],
                    inc_entry['unique_keys'],
                    process_id
                )
                total_loaded += inserted

        logging.info(f"ETL process completed successfully process_id={process_id}. Total records loaded: {total_loaded}")

    except Exception as e:
        error_message = str(e)
        logging.error(f"ETL failed (process_id={process_id}): {error_message}")

    finally:
        if process_id is not None:
            end_etl_process(engine, config, process_id, total_loaded, error_message)
            archive_data_files(config, process_id)

if __name__ == "__main__":
    main()
