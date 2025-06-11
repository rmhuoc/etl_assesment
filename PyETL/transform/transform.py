import pandas as pd
from utils.encryptation import load_key, encrypt_value
from cryptography.fernet import Fernet
import logging

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




def encrypt_csv_to_new_file(file_path, output_path, encryption_config):
    df = pd.read_csv(file_path)

    if not encryption_config.get("enabled", False):
        logging.info("Encryption is disabled in config.")
        return

    key_path = encryption_config["key_path"]
    columns = encryption_config["columns_to_encrypt"]

    key = load_key(key_path)
    fernet = Fernet(key)

    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: encrypt_value(x, fernet))

    df.to_csv(output_path, index=False)
    logging.info(f"Encrypted CSV written to '{output_path}'")
