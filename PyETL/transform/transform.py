import pandas as pd
from utils.encryptation import load_key, encrypt_value
from cryptography.fernet import Fernet
import logging

def data_encryptation(file_path, output_path, encryption_config):
    """
    Load a CSV file, encrypt specified columns, and write the result to a new CSV file.

    Parameters:
        file_path (str): Path to the input CSV file.
        output_path (str): Path where the encrypted CSV will be saved.
        encryption_config (dict): Configuration dictionary with:
            - 'enabled' (bool): Whether encryption is enabled.
            - 'key_path' (str): File path to the encryption key.
            - 'columns_to_encrypt' (list of str): List of column names to encrypt.

    Behavior:
        - Reads the CSV into a DataFrame.
        - If encryption is disabled in config, logs info and skips encryption.
        - Encrypts specified columns using Fernet.
        - Saves encrypted DataFrame to output CSV without index.
        - Logs a message upon successful writing.
    """
    #logging.info(f"file path'{file_path}'")
    df = pd.read_csv(file_path)

    if not encryption_config.get("enabled", False):
        logging.info("Encryption is disabled in config.")
        return

    key_path = encryption_config["key_path"]
    columns = encryption_config["columns_to_encrypt"]

    # Load the encryption key from file
    key = load_key(key_path)
    fernet = Fernet(key)

    # Encrypt specified columns if they exist in the DataFrame
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: encrypt_value(x, fernet))

    # Write the encrypted DataFrame to CSV
    df.to_csv(output_path, index=False)
    logging.info(f"Encrypted CSV written to '{output_path}'")
