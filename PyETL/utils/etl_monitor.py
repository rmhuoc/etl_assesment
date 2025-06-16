import logging
from datetime import datetime
from sqlalchemy import text


def start_etl_process(engine, config):
    """
    Initialize an ETL process by inserting a new record in the ETL tracking table.

    This function retrieves the next value from a specified sequence to use as
    a unique process_id, inserts a new row into the tracking table with status "RUNNING",
    and records the start time.

    Parameters:
        engine (sqlalchemy.Engine): SQLAlchemy engine connected to the target database.
        config (dict): Configuration dictionary containing 'load_process' keys such as schema,
                       table_name, and sequence_name.

    Returns:
        int: The generated process_id from the sequence.
    """
    load_proc = config['load_process']
    schema = load_proc['schema']
    table_name = load_proc['table_name']
    sequence = load_proc['sequence_name']
    
    full_table = f"{schema}.{table_name}"
    full_sequence = f"{schema}.{sequence}"

    with engine.connect() as conn:
        # Get the next process_id from the sequence
        result = conn.execute(text(f"SELECT nextval('{full_sequence}')"))
        process_id = result.scalar()

        # Insert a new ETL process record with status RUNNING and start timestamp
        conn.execute(text(f"""
            INSERT INTO {full_table} (process_id, start_time, status)
            VALUES (:process_id, :start_time, :status)
        """), {
            "process_id": process_id,
            "start_time": datetime.now(),
            "status": "RUNNING"
        })
        conn.commit()
    return process_id


def end_etl_process(engine, config, process_id, num_records_loaded, error_message=None):
    """
    Finalize an ETL process by updating the tracking record with end time, status,
    number of records loaded, and an optional error message.

    Parameters:
        engine (sqlalchemy.Engine): SQLAlchemy engine connected to the target database.
        config (dict): Configuration dictionary containing 'load_process' keys such as schema
                       and table_name.
        process_id (int): The unique identifier of the ETL process being finalized.
        num_records_loaded (int): The count of records successfully loaded during the process.
        error_message (str, optional): Error message if the ETL process failed; defaults to None.

    Returns:
        None
    """
    load_proc = config['load_process']
    schema = load_proc['schema']
    table_name = load_proc['table_name']
    full_table = f"{schema}.{table_name}"

    # Determine status based on presence of error message
    status = "COMPLETED" if error_message is None else "ERROR"

    with engine.connect() as conn:
        # Update the ETL tracking record with end time, status, record count, and error info
        conn.execute(text(f"""
            UPDATE {full_table}
            SET end_time = :end_time,
                num_records_loaded = :num_records_loaded,
                status = :status,
                error_message = :error_message
            WHERE process_id = :process_id
        """), {
            "end_time": datetime.now(),
            "num_records_loaded": num_records_loaded,
            "status": status,
            "error_message": error_message,
            "process_id": process_id
        })
        conn.commit()
