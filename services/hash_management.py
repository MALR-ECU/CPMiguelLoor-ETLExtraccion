import hashlib
import logging

def calculate_file_hash(file_data):
    """Calcula el hash SHA-256 del contenido de un archivo."""
    return hashlib.sha256(file_data).hexdigest()

def is_file_already_processed(file_hash, blob_hash_client):
    """Verifica si un hash ya está registrado en el archivo de hashes."""
    try:
        hash_data = blob_hash_client.download_blob().readall().decode("utf-8")
        return file_hash in hash_data.splitlines()
    except Exception as e:
        logging.error(f"Error al verificar el hash: {e}")
        return False

def register_file_hash(file_hash, blob_hash_client):
    """Registra un nuevo hash en el archivo de hashes."""
    try:
        existing_hashes = blob_hash_client.download_blob().readall().decode("utf-8")
        updated_hashes = f"{existing_hashes}\n{file_hash}".strip()
        blob_hash_client.upload_blob(updated_hashes, overwrite=True)
    except Exception as e:
        logging.error(f"Error al registrar el hash: {e}")