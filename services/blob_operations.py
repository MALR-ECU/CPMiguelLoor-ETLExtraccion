import logging
import os
from azure.storage.blob import BlobServiceClient
from azure.storage.fileshare import ShareServiceClient

def copy_processed_files_to_blob(file_share_connection_string, file_share_name, blob_connection_string, blob_container_name, hash_blob_client):
    """
    Copia todos los archivos procesados (registrados en processed_hashes.txt) desde Azure File Share al Blob Storage
    y elimina los archivos del recurso compartido después de la copia exitosa.
    """
    try:
        # Inicializar clientes de Azure
        share_service_client = ShareServiceClient.from_connection_string(file_share_connection_string)
        share_client = share_service_client.get_share_client(file_share_name)

        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        blob_container_client = blob_service_client.get_container_client(blob_container_name)

        # Leer el archivo de hashes desde el Blob Storage
        processed_hashes = hash_blob_client.download_blob().readall().decode("utf-8").splitlines()

        for file_hash in processed_hashes:
            logging.info(f"Buscando archivo con hash: {file_hash}")

            for item in share_client.list_directories_and_files():
                if item["is_directory"]:
                    continue  # Ignorar directorios

                file_name = item["name"]
                file_client = share_client.get_file_client(file_name)
                file_data = file_client.download_file().readall()

                # Calcular el hash del archivo actual
                from services.hash_management import calculate_file_hash
                current_file_hash = calculate_file_hash(file_data)

                if current_file_hash == file_hash:
                    logging.info(f"El archivo {file_name} corresponde al hash registrado. Procediendo a copiar.")
                else:
                    logging.info(f"El archivo {file_name} tiene un hash diferente. Se copiará con un nuevo nombre.")

                # Comprobar si el archivo ya existe en el contenedor Blob Storage
                blob_client = blob_container_client.get_blob_client(file_name)
                if blob_client.exists():
                    existing_metadata = blob_client.get_blob_properties().metadata
                    existing_hash = existing_metadata.get("file_hash", "")

                    if existing_hash == current_file_hash:
                        logging.info(f"El archivo {file_name} ya existe en el Blob Storage con el mismo hash. Se omitirá.")
                        continue
                    else:
                        file_name = f"{os.path.splitext(file_name)[0]}-{current_file_hash}{os.path.splitext(file_name)[1]}"
                        blob_client = blob_container_client.get_blob_client(file_name)
                        logging.info(f"El archivo será guardado como {file_name}.")

                # Subir el archivo al Blob Storage con el hash como metadato
                blob_client.upload_blob(file_data, overwrite=True, metadata={"file_hash": current_file_hash})
                logging.info(f"Archivo {file_name} copiado exitosamente al contenedor {blob_container_name}.")

                # Eliminar el archivo del recurso compartido después de la copia
                file_client.delete_file()
                logging.info(f"Archivo {file_name} eliminado del recurso compartido.")
    except Exception as e:
        logging.error(f"Error al copiar y eliminar archivos procesados: {e}")