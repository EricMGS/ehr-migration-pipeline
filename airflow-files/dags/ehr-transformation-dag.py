from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from tasks.csv_schema_validation import SchemaValidationOperator
from tasks.conversion import ConversionOperator
from tasks.import_data import ImportOperator
from tasks.get_files import DownloadOperator
from tasks.csv_schema_validation import ImportValidationOperator
from tasks.categorization import CategorizationOperator
import json, os, shutil
import pandas as pd

#Project definitions
ORDER_ID = "{{ dag_run.conf['order_id'] }}"
AIRFLOW_HOME = '/home/user/airflow/'    #Set this to the path where the Airflow project was created
PROJECT_HOME = AIRFLOW_HOME + 'project/'
LOG_PATH = AIRFLOW_HOME + "logs/"
CURRENT_ORDER = PROJECT_HOME + 'migration_order.json'
MIGRATIONS_TABLENAME = "migration_orders" #Set the tablename where migration orders are stored in postgres
#Models
VALIDATION_MODELS = PROJECT_HOME + 'models/'
CONVERSION_MODELS = PROJECT_HOME + 'definitions/conversion_models/'
IMPORT_MODELS = PROJECT_HOME + 'definitions/import_schemas/'
#Connectors
AWS_CONNECTOR = "aws_conn"
POSTGRES_CONNECTOR = "postgres_conn"
#S3 Definitions
BUCKET_NAME = 'ehr-transformation-pipeline'
S3_MIGRATION_PATH = "migrations/migration_{}/".format(ORDER_ID)
S3_CATEGORIZED_PATH = S3_MIGRATION_PATH + 'categorized_files/'
S3_CONVERTED_PATH = S3_MIGRATION_PATH + 'converted_files/'
#Temporary CSV files folders
CACHE_FOLDER = AIRFLOW_HOME + 'migrations'
DOWNLOAD_PATH = CACHE_FOLDER + "/migration_{}/".format(ORDER_ID)
INPUT_PATH = CACHE_FOLDER + '/migration_{}/input_files/'.format(ORDER_ID)
CATEGORIZED_PATH = CACHE_FOLDER + '/migration_{}/categorized_files/'.format(ORDER_ID)
CONVERTED_PATH = CACHE_FOLDER + '/migration_{}/converted_files/'.format(ORDER_ID)

def get_migration_order(order_id, conn, tablename, path):
    hook = PostgresHook(postgres_conn_id = conn)
    df = hook.get_pandas_df(sql = 'SELECT * FROM {} WHERE order_id = {}'.format(tablename, order_id))
    json_text = df.to_json(orient = 'records', date_format = 'iso')[1:-1]
    json_file = open(path,'w')
    json_file.write(json_text)
    json_file.close()

default_args = {
    'owner': 'eric_souza',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 26),
    'retries': 0,
    }
with DAG(
    'ehr-transformation-pipeline',
    default_args = default_args,
    schedule = None, 
    catchup = False
    ) as dag:

    reset_cache = PythonOperator(
        task_id = 'reset_cache',
        python_callable = lambda : shutil.rmtree(CACHE_FOLDER, ignore_errors = True)
    )

    get_migration_order = PythonOperator(
        task_id = 'get_migration_order',
        python_callable = get_migration_order,
        op_kwargs = {
            "order_id": ORDER_ID,
            "conn": POSTGRES_CONNECTOR,
            "tablename": MIGRATIONS_TABLENAME,
            "path": CURRENT_ORDER
        }
    )

    get_files = PythonOperator(
        task_id = 'get_files',
        python_callable = DownloadOperator,
        op_kwargs = {
            "connector": AWS_CONNECTOR,
            "bucket_name": BUCKET_NAME,
            "path": S3_MIGRATION_PATH,
            "destination": AIRFLOW_HOME
        }
    )
    
    validate_schema = PythonOperator(
        task_id = 'validate_schema',
        python_callable = SchemaValidationOperator,
        op_kwargs = {
            "migration_order": CURRENT_ORDER,
            "files_path": INPUT_PATH,
            "schemas_path": VALIDATION_MODELS,
            "log_path": LOG_PATH,
            "db_connector": POSTGRES_CONNECTOR,
            "tablename": MIGRATIONS_TABLENAME
            }
    )

    categorization = PythonOperator(
        task_id = 'categorization',
        python_callable = CategorizationOperator,
        op_kwargs = {
            "migration_order": CURRENT_ORDER,
            "files_path": INPUT_PATH,
            "schemas_path": VALIDATION_MODELS,
            "out_path": CATEGORIZED_PATH,
            "log_path": LOG_PATH,
            "aws_connector": AWS_CONNECTOR,
            "aws_bucket": BUCKET_NAME,
            "aws_path": S3_CATEGORIZED_PATH,
            "db_connector": POSTGRES_CONNECTOR,
            "tablename": MIGRATIONS_TABLENAME
        }
    )

    conversion = PythonOperator(
        task_id = 'conversion',
        python_callable = ConversionOperator,
        op_kwargs = {
            "migration_order": CURRENT_ORDER,
            "files_path": CATEGORIZED_PATH,
            "schemas_path": CONVERSION_MODELS,
            "out_path": CONVERTED_PATH,
            "log_path": LOG_PATH,
            "aws_connector": AWS_CONNECTOR,
            "aws_bucket": BUCKET_NAME,
            "aws_path": S3_CONVERTED_PATH,
            "db_connector": POSTGRES_CONNECTOR,
            "tablename": MIGRATIONS_TABLENAME
        }
    )

    validate_import = PythonOperator(
        task_id = 'validate_import',
        python_callable = ImportValidationOperator,
        op_kwargs = {
            "migration_order": CURRENT_ORDER,
            "files_path": CONVERTED_PATH,
            "schemas_path": IMPORT_MODELS,
            "log_path": LOG_PATH,
            "db_connector": POSTGRES_CONNECTOR,
            "tablename": MIGRATIONS_TABLENAME
            }
    )

    import_files = PythonOperator(
        task_id = 'import_files',
        python_callable = ImportOperator,
        op_kwargs = {
            "connector": POSTGRES_CONNECTOR,
            "files_path": CONVERTED_PATH,
            "models_path": IMPORT_MODELS,
            "log_path": LOG_PATH,
            "order_table": MIGRATIONS_TABLENAME,
            "order_id": ORDER_ID,
            "migration_order": CURRENT_ORDER
        }
    )

reset_cache >> get_migration_order >> get_files >> validate_schema >> categorization >> conversion >> validate_import >> import_files