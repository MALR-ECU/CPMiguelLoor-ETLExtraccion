import logging
import os
import pandas as pd
from azure.storage.fileshare import ShareServiceClient
from azure.storage.blob import BlobServiceClient, BlobClient
from services.hash_management import calculate_file_hash, is_file_already_processed, register_file_hash

# Variables de entorno
blob_storage_connection_string = os.environ["AZURE_BLOB_CONNECTION_STRING"]
blob_hash_container_name = os.environ["AZURE_CONTAINER_BLOB_HASH"]
blob_hash_file_name = os.environ["AZURE_FILE_BLOB_HASH"]

def process_files_from_share(file_share_connection_string, file_share_name):
    logging.info("Inicializando cliente de Azure File Share.")
    share_service_client = ShareServiceClient.from_connection_string(file_share_connection_string)
    share_client = share_service_client.get_share_client(file_share_name)
    
    blob_service_client = BlobServiceClient.from_connection_string(blob_storage_connection_string)
    blob_hash_container_client = blob_service_client.get_container_client(blob_hash_container_name)
    blob_hash_client = blob_hash_container_client.get_blob_client(blob_hash_file_name)

    # Verificar si el archivo de hashes existe, si no, crearlo
    if not blob_hash_client.exists():
        logging.info(f"El archivo de hashes {blob_hash_file_name} no existe. Creando uno nuevo.")
        blob_hash_client.upload_blob("", overwrite=True)

    consolidated_df = pd.DataFrame()
    temp_file_hashes = []  # Lista para almacenar los hashes temporalmente

    for item in share_client.list_directories_and_files():
        if item["is_directory"]:
            continue  # Ignorar directorios

        file_name = item["name"]
        logging.info(f"Procesando archivo: {file_name}")

        try:
            # Descargar archivo
            file_client = share_client.get_file_client(file_name)
            download_stream = file_client.download_file()
            file_data = download_stream.readall()

            # Calcular hash del archivo
            file_hash = calculate_file_hash(file_data)

            # Verificar si el archivo ya fue procesado
            if is_file_already_processed(file_hash, blob_hash_client):
                logging.info(f"El archivo {file_name} ya fue procesado anteriormente. Ignorando.")
                continue

            # Leer archivo Excel
            df = pd.read_excel(file_data)

            # Concatenar DataFrame procesado
            consolidated_df = pd.concat([consolidated_df, df], ignore_index=True)

            # Almacenar hash en la lista temporal
            temp_file_hashes.append(file_hash)
        except Exception as e:
            logging.error(f"Error al procesar {file_name}: {e}")
            continue

    consolidated_df = consolidated_df.fillna('')  # Reemplazar NaN por cadenas vacías
    return consolidated_df, temp_file_hashes