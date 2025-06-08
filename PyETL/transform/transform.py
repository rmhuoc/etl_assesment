import pandas as pd
from utils.encryptation import load_key, encrypt_value
from cryptography.fernet import Fernet

def encrypt_dataframe(df, encryption_config):
    if not encryption_config.get('enabled', False):
        return df

    key_path = encryption_config['key_path']
    columns = encryption_config['columns']

    key = load_key(key_path)
    fernet = Fernet(key)

    df_copy = df.copy()
    for col in columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].apply(lambda x: encrypt_value(x, fernet))
    return df_copy
