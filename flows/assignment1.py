from prefect import flow, task
from prefect_azure import AzureBlobStorageCredentials
from prefect_azure.blob_storage import blob_storage_download
from azure.storage.blob import BlobServiceClient
import psycopg2
import csv
from prefect_sqlalchemy import DatabaseCredentials, SyncDriver
from prefect_sqlalchemy.database import sqlalchemy_execute
from prefect.task_runners import SequentialTaskRunner

""" 
pip install "prefect-azure[blob_storage]" prefect-sqlalchemy azure-storage-blob==12.3.2 psycopg2-binary pandas aiohttp
"""


@task(name="check_file")
def check_new_file_in_blob_storage() -> bool:
    AZURE_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey' \
                                      '=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr' \
                                      '/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;'

    AZURE_CONTAINER_NAME = 'files-container'

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
    blobs = container_client.list_blobs()

    processed_files = set()
    new_blob_exists = False
    for blob in blobs:
        if blob.name not in processed_files:
            new_blob_exists = True
            processed_files.add(blob.name)

    return new_blob_exists


@task(name="conn_blob_storage")
def connect_to_blob_storage():
    AZURE_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey' \
                                      '=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr' \
                                      '/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;'

    blob_storage_credentials = AzureBlobStorageCredentials(
        connection_string=AZURE_STORAGE_CONNECTION_STRING,
    )
    return blob_storage_credentials


@task(name="conn_postgres")
def create_table(sqlalchemy_credentials) -> None:
    sqlalchemy_execute(
        "CREATE TABLE IF NOT EXISTS users (id NUMERIC PRIMARY KEY, username TEXT, description TEXT);",
        sqlalchemy_credentials,
    )


@task(name="load_csv_to_postgres")
def load_csv_to_postgres() -> None:
    PG_HOST = 'postgres'
    PG_PORT = '5432'
    PG_DB = 'prefect'
    PG_USER = 'prefect'
    PG_PASSWORD = 'prefect'

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    file_path = './data/users.csv'
    table_name = 'users'
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        with conn.cursor() as cursor:
            cursor.copy_from(file, table_name, sep=',', null='NULL')
        conn.commit()
    conn.close()


@flow(task_runner=SequentialTaskRunner(), name="from_blob_to_database", )
def from_blob_to_database():
    blob_storage_credentials = connect_to_blob_storage()

    test = check_new_file_in_blob_storage()
    if test:
        data = blob_storage_download(
            container="files-container",
            blob="users.csv",
            blob_storage_credentials=blob_storage_credentials,
        )
        file_path = './data/users.csv'
        with open(file_path, 'w') as file:
            file.write(data.decode('utf-8'))

        sqlalchemy_credentials = DatabaseCredentials(
            driver=SyncDriver.POSTGRESQL_PSYCOPG2,
            username="prefect",
            password="prefect",
            database="prefect",
            host="postgres",
            port=5432
        )
        sqlalchemy_execute(
            "CREATE TABLE IF NOT EXISTS users (id NUMERIC PRIMARY KEY, username TEXT, description TEXT);",
            sqlalchemy_credentials,
        )

        load_csv_to_postgres()


if __name__ == '__main__':
    from_blob_to_database()
