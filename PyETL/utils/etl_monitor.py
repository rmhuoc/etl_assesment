import logging
from datetime import datetime
from sqlalchemy import text


def start_etl_process(engine, config):
    load_proc = config['load_process']
    schema = load_proc['schema']
    table_name = load_proc['table_name']
    sequence = load_proc['sequence_name']
    
    full_table = f"{schema}.{table_name}"
    full_sequence = f"{schema}.{sequence}"  # 👈 Aquí se concatena schema + sequence

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT nextval('{full_sequence}')"))
        process_id = result.scalar()

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
    load_proc = config['load_process']
    schema = load_proc['schema']
    table_name = load_proc['table_name']
    full_table = f"{schema}.{table_name}"

    status = "COMPLETED" if error_message is None else "ERROR"

    with engine.connect() as conn:
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
