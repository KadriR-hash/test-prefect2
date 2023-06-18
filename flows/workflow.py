import csv

import psycopg2
from prefect import flow, task, get_run_logger
from prefect_azure import AzureBlobStorageCredentials
from prefect_azure.blob_storage import blob_storage_download
from prefect_sqlalchemy import DatabaseCredentials, SyncDriver
from prefect_sqlalchemy.database import sqlalchemy_execute
from prefect.blocks.system import JSON

# from prefect.client.orchestration import PrefectClient, get_client
# async with get_client() as client:
#      response = await client.hello()

dag = {
    "flow_id": "from_blob_to_database_flow",
    "tasks": [
        {
            "operation_class": "postgresql",
            "operation": "load_csv",
            "dependencies": ["create_table"],
            "task_id": "load_csv_to_postgres",
            "params": {
                "PG_HOST": "postgres",
                "PG_PORT": "5432",
                "PG_DB": "prefect",
                "PG_USER": "prefect",
                "PG_PASSWORD": "prefect",
                "file_path": "./data/users.csv",
                "table_name": "users"
            }
        },
        {
            "operation_class": "blob_storage",  # airflow will not need this field.
            "operation": "connect",  # airflow will use this field differently.
            "dependencies": [],  # provide downstream tasks (empty = first task).
            "task_id": "conn_blob_storage",
            "params": {
                "connection_string":
                    'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey'
                    '=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr'
                    '/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;',
                "container": "files-container",
            }
        },
        {
            "operation_class": "blob_storage",
            "operation": "download_locally",
            "dependencies": ["download_blob"],
            "task_id": "download_blob_to_local",
            "params": {
                "file_path": './data/users.csv',
            }
        },
        {
            "operation_class": "blob_storage",
            "operation": "download",
            "dependencies": ["conn_blob_storage"],
            "task_id": "download_blob",
            "params": {
                "container": "files-container",
                "blob": "users.csv",
            }
        },
        {
            "operation_class": "postgresql",
            "operation": "connect",
            "dependencies": ["download_blob_to_local"],
            "task_id": "conn_to_postgres",
            "params": {
                "PG_HOST": "postgres",
                "PG_PORT": "5432",
                "PG_DB": "prefect",
                "PG_USER": "prefect",
                "PG_PASSWORD": "prefect",
            },
        },
        {
            "operation_class": "postgresql",
            "operation": "sql_statements",
            "dependencies": ["conn_to_postgres"],
            "task_id": "create_table",
            "params": {
                "statement": "CREATE TABLE IF NOT EXISTS users (id NUMERIC PRIMARY KEY, username TEXT, description "
                             "TEXT);",
            }
        }
    ]
}


class BlobStorage:
    mapper = {
        "connect": "connect_blob_storage",
        "download_locally": "download_blob_local",
        "download": "blob_download"
    }
    """    
    def __init__(self, connection_string):
        self.connection_string = connection_string
        =>
        error when trying to use self in the function:
        -> raise ParameterBindError.from_bind_failure(fn, exc, call_args, call_kwargs)
    """

    @staticmethod
    @task
    def connect_blob_storage(config: dict, input_value):
        blob_storage_credentials = AzureBlobStorageCredentials(
            connection_string=config['connection_string']
        )
        return blob_storage_credentials

    @staticmethod
    @task
    def download_blob_local(config: dict, input_value):
        with open(config['file_path'], 'w') as file:
            file.write(input_value.decode('utf-8'))
        return input_value

    # blob_storage_download is a task
    # non need to use the decorator @task
    @staticmethod
    def blob_download(config: dict, input_value):
        """
        blob_storage_download is a task for interacting with Azure Blob Storage
        """
        container = config['container']
        blob = config['blob']

        if input_value:
            data = blob_storage_download(
                container=container,
                blob=blob,
                blob_storage_credentials=input_value,
            )
            return data
        else:
            ValueError("Input Value Provided is not of type AzureBlobStorageCredentials !")


class Postgresql:
    mapper = {
        "connect": "postgresql_credentials",
        "load_csv": "csv_to_postgres",
        "sql_statement": "exec_sql_statement"
    }

    @task
    def postgresql_credentials(config: dict, input_value):
        sqlalchemy_credentials = DatabaseCredentials(
            driver=SyncDriver.POSTGRESQL_PSYCOPG2,
            username=config['PG_USER'],
            password=config['PG_PASSWORD'],
            database=config['PG_DB'],
            host=config['PG_HOST'],
            port=config['PG_PORT']
        )
        return sqlalchemy_credentials

    @staticmethod
    @task
    def csv_to_postgres(config: dict, input_value):
        conn = psycopg2.connect(
            user=config['PG_USER'],
            password=config['PG_PASSWORD'],
            database=config['PG_DB'],
            host=config['PG_HOST'],
            port=config['PG_PORT'],
        )
        with open(config['file_path'], 'r') as file:
            reader = csv.reader(file)
            next(reader)
            with conn.cursor() as cursor:
                cursor.copy_from(file, config['table_name'], sep=',', null='NULL')
            conn.commit()
        conn.close()
        return input_value

    @staticmethod
    def exec_sql_statement(config: dict, input_value):
        sqlalchemy_execute(
            statement=config['statement'],
            sqlalchemy_credentials=input_value,
        )
        return input_value


class_mapper = {
    "blob_storage": BlobStorage,
    "postgresql": Postgresql
}


def create_instance(operation_class: str):
    if operation_class in class_mapper:
        return class_mapper[operation_class]()
    else:
        raise ValueError("Invalid ID: {}".format(operation_class))


def is_all_elements_present(list1, list2):
    for element in list1:
        if element not in list2:
            return False
    return True


# def save_value_globals(identifier, value):
#     globals()[identifier] = value
# def read_value_globals(identifier):
#     if identifier in globals():
#         return globals()[identifier]
#     else:
#         return None
# def read_values_for_a_task(task):
#     values = {}
#     for dependency in task['dependencies']:
#         values[dependency] = read_value_globals(dependency)
#     return values

# todo : how to config flow's and task's metadata dynamically.
# todo : separate blob conn and postgres conn from the workflow! => using blocks.
@flow
def pipeline_generator(dag: dict):
    logger = get_run_logger()
    instantiated_tasks = []
    tasks_ids = []

    tasks = dag['tasks']

    for task in tasks:
        tasks_ids.append(task['task_id'])
    logger.info(f"INFO : task_ids : {tasks_ids}")

    input_value = None

    while True:
        for task in tasks:
            if (task['task_id'] not in instantiated_tasks) and (not len(task['dependencies'])
                                                                or is_all_elements_present(task['dependencies'],
                                                                                           instantiated_tasks)):
                logger.info(f"INFO : Task ID : {task['task_id']}")
                class_instance = create_instance(task['operation_class'])

                if task['operation'] in class_instance.mapper:
                    method_name = class_instance.mapper[task['operation']]
                    method = getattr(class_instance, method_name)

                    logger.info(f"INFO : task params : {task['params']}")
                    logger.info(f"INFO : input value : {input_value}")

                    returned_value = method(task['params'], input_value)

                    logger.info(f"INFO : returned value : {returned_value}")
                    input_value = returned_value

                    instantiated_tasks.append(task['task_id'])
                    logger.info(f"INFO : instantiated tasks = {instantiated_tasks}")

        if is_all_elements_present(instantiated_tasks, tasks_ids) and len(instantiated_tasks) == len(tasks_ids):
            break


def load_block():
    """
        Here is the logic to accept the json from the backend.
    """
    # json_block = JSON(value=dag)
    # json_block.save(name="dag-numer-uno")
    # json_block = JSON.load("dag-numer-uno")
    return JSON.load("dag-numer-uno")


if __name__ == '__main__':
    """
        Here will be the logic to save the block before.
    """
    json_block = load_block()
    pipeline_generator(dag)
