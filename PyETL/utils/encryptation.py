#utils/encryptation.py
from cryptography.fernet import Fernet
import pandas as pd

import logging

def load_key(key_path):
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
    if pd.isna(value):
        return value
    return fernet.encrypt(str(value).encode()).decode()

def decrypt_value(value, fernet):
    if pd.isna(value):
        return value
    return fernet.decrypt(value.encode()).decode()
