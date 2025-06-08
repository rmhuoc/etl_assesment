# src/load.py
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from concurrent.futures import ThreadPoolExecutor
import logging

def get_engine(db_config):
    url = f"{db_config['dialect']}://{db_config['user']}:{db_config['password']}@" \
          f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(url)

def clean_strings(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: x.encode('utf-8', 'replace').decode('utf-8', 'replace') if isinstance(x, str) else x)
    return df
    
def load_data(df, engine, table_name, schema=None):
    df.reset_index(inplace=True)
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists='append',
        index=False
    )
