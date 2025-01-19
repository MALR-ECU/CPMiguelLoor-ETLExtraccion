# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(extraerdatos) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import azure.functions as func
import logging
import os
import pandas as pd

from azure.storage.fileshare import ShareServiceClient
from azure.storage.blob import BlobServiceClient

from services.process_files import process_files_from_share
from services.sql_operations import insert_into_sql
from services.blob_operations import copy_processed_files_to_blob

# Configuración de conexiones
file_share_connection_string = os.environ["AZURE_FILE_SHARE_CONNECTION_STRING"]
file_share_name = os.environ["AZURE_FILE_SHARE"]  # Nombre del recurso compartido
blob_storage_connection_string = os.environ["AZURE_BLOB_CONNECTION_STRING"]
blob_container_name = os.environ["AZURE_CONTAINER_BLOB"]
blob_hash_container_name = os.environ["AZURE_CONTAINER_BLOB_HASH"]
blob_hash_file_name = os.environ["AZURE_FILE_BLOB_HASH"]

# Inicializar el Blueprint
process_files_to_landing = func.Blueprint()

@process_files_to_landing.route(route="process_excel", auth_level=func.AuthLevel.FUNCTION)
def process_excel(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Procesando archivos Excel desde Azure File Share e insertando en SQL Server.")

    try:

        # Inicializar cliente para el archivo de hashes
        blob_service_client = BlobServiceClient.from_connection_string(blob_storage_connection_string)
        blob_hash_client = blob_service_client.get_blob_client(blob_hash_container_name, blob_hash_file_name)

        # Cargar el contenido original de processed_hashes.txt
        try:
            original_hashes = blob_hash_client.download_blob().content_as_text().splitlines()
        except Exception as e:
            logging.warning(f"No se pudo cargar el archivo de hashes existente: {e}")
            original_hashes = []

        # Procesar los archivos desde Azure File Share y obtener los hashes generados
        consolidated_df, temp_file_hashes = process_files_from_share(
            file_share_connection_string,
            file_share_name
        )
        
        if consolidated_df.empty:
            logging.info("No se encontraron datos para consolidar.")
            return func.HttpResponse("No se encontraron datos para consolidar.", status_code=200)

        # Intentar insertar datos en SQL Server
        try:
            insert_into_sql(consolidated_df)
        except Exception as e:
            logging.error(f"Error al insertar datos en SQL Server: {e}")
            return func.HttpResponse(f"Error en el proceso SQL: {e}", status_code=500)

        # Actualizar el archivo de hashes en Blob Storage
        try:
            updated_hashes = original_hashes + temp_file_hashes
            blob_hash_client.upload_blob("\n".join(updated_hashes), overwrite=True)
        except Exception as e:
            logging.error(f"Error al actualizar el archivo de hashes: {e}")
            return func.HttpResponse(f"Error al actualizar el archivo de hashes: {e}", status_code=500)

        # Copiar archivos procesados al Blob Storage
        try:
            copy_processed_files_to_blob(
                file_share_connection_string,
                file_share_name,
                blob_storage_connection_string,
                blob_container_name,
                blob_hash_client
            )
        except Exception as e:
            logging.error(f"Error al copiar archivos al Blob Storage: {e}")
            return func.HttpResponse(f"Error en la copia a Blob Storage: {e}", status_code=500)

        return func.HttpResponse("Consolidación completada, archivos copiados al Blob Storage e inserción en SQL Server exitosa.", status_code=200)

    except Exception as e:
        logging.error(f"Error inesperado en el proceso: {e}")
        return func.HttpResponse(f"Error inesperado en el proceso: {e}", status_code=500)