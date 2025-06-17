import pandas as pd
from utils.encryptation import load_key, encrypt_value
from cryptography.fernet import Fernet
import logging

def encrypt_dataframe(df, encryption_config):
    """
    Encrypt specified columns of a pandas DataFrame using Fernet symmetric encryption.

    Parameters:
        df (pandas.DataFrame): Input DataFrame to encrypt.
        encryption_config (dict): Configuration dictionary with:
            - 'enabled' (bool): Whether encryption is enabled.
            - 'key_path' (str): File path to the encryption key.
            - 'columns' (list of str): List of column names to encrypt.

    Returns:
        pandas.DataFrame: A copy of the DataFrame with specified columns encrypted.
        If encryption is disabled, returns the original DataFrame unchanged.
    """
    if not encryption_config.get('enabled', False):
        # Encryption not enabled - return original DataFrame
        return df

    key_path = encryption_config['key_path']
    columns = encryption_config['columns']

    # Load encryption key from file
    key = load_key(key_path)
    fernet = Fernet(key)

    # Work on a copy to avoid modifying original DataFrame
    df_copy = df.copy()

    # Encrypt each specified column if present
    for col in columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].apply(lambda x: encrypt_value(x, fernet))

    return df_copy


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
