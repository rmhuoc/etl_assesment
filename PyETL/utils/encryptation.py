from cryptography.fernet import Fernet
import pandas as pd
import logging


def load_key(key_path):
    """
    Load a symmetric encryption key from a file.

    Parameters:
        key_path (str): Path to the file containing the encryption key.

    Returns:
        bytes: The loaded key bytes.

    Raises:
        FileNotFoundError: If the key file does not exist.
        Exception: For any other error during key loading.
    """
    try:
        with open(key_path, "rb") as key_file:
            key = key_file.read()
            logging.info(f"Encryption key successfully loaded from '{key_path}'")
            return key
    except FileNotFoundError:
        logging.error(f"Encryption key file not found at '{key_path}'")
        raise
    except Exception as e:
        logging.error(f"Error loading encryption key from '{key_path}': {e}")
        raise


def encrypt_value(value, fernet):
    """
    Encrypt a single value using Fernet symmetric encryption.

    Handles pandas NaN values by returning them unchanged.

    Parameters:
        value (any): The value to encrypt.
        fernet (Fernet): An initialized Fernet encryption object.

    Returns:
        str or any: The encrypted value as a string, or the original value if NaN.
    """
    if pd.isna(value):
        # Do not encrypt NaN values, just return them as is
        return value
    # Convert value to string, encode, encrypt, and decode back to string
    return fernet.encrypt(str(value).encode()).decode()


def decrypt_value(value, fernet):
    """
    Decrypt a single value using Fernet symmetric encryption.

    Handles pandas NaN values by returning them unchanged.

    Parameters:
        value (str or any): The encrypted value to decrypt.
        fernet (Fernet): An initialized Fernet encryption object.

    Returns:
        str or any: The decrypted string value, or the original value if NaN.
    """
    if pd.isna(value):
        # Do not decrypt NaN values, just return them as is
        return value
    # Decode string, decrypt bytes, and decode decrypted bytes to string
    return fernet.decrypt(value.encode()).decode()
