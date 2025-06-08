#utils/encryptation.py
from cryptography.fernet import Fernet

def load_key(path="secret.key"):
    with open(path, "rb") as key_file:
        return key_file.read()

def encrypt_value(value, fernet):
    return fernet.encrypt(value.encode()).decode()

def decrypt_value(encrypted_value, fernet):
    return fernet.decrypt(encrypted_value.encode()).decode()
