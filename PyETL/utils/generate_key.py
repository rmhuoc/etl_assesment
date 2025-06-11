from cryptography.fernet import Fernet
import os

# Asegura que el directorio config/ existe
os.makedirs("config", exist_ok=True)

key = Fernet.generate_key()

with open("config/secret.key", "wb") as key_file:
    key_file.write(key)

print("✅ Clave Fernet generada en 'config/secret.key'")
