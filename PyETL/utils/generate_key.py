from cryptography.fernet import Fernet
import os

# This script is only used to generate the Fernet secret key,
# it is not called or imported from the main code.

# Ensure the 'config' directory exists to store the encryption key
os.makedirs("config", exist_ok=True)

# Generate a Fernet key (symmetric key for encryption and decryption)
key = Fernet.generate_key()

# Save the generated key to a file inside the 'config' directory
with open("config/secret.key", "wb") as key_file:
    key_file.write(key)

print("✅ Fernet key generated in 'config/secret.key'")
