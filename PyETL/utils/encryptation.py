#utils/encryptation.py
from cryptography.fernet import Fernet

def load_key(path):
    with open(path, "rb") as key_file:
        return key_file.read()

def encrypt_value(value, fernet):
    if pd.isna(value):
        return value
    return fernet.encrypt(str(value).encode()).decode()

def decrypt_value(value, fernet):
    if pd.isna(value):
        return value
    return fernet.decrypt(value.encode()).decode()
