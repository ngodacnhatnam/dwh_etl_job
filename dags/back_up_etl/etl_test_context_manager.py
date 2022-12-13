from airflow.operators.python import PythonOperator
from airflow import DAG
import pendulum
from adhoc.connect import ProcessingData
from textwrap import dedent
import json
import time
import pandas as pd
from pendulum import datetime
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
# [START tutorial]
# [START import_module]

# The DAG object; we'll need this to instantiate a DAG

# variables
TABLE_NAME = "STG_DANH_MUC_TT_PHI_PHUC_LOI"
SCHEMA_NAME = "STG"
DB_TYPE="postgres"
CONNECTION="dwh_etl"
"""
# ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

# [END import_module]

# [START extract_function]


def extract(ti):
    file_path = f'/home/airflow/airflow/dags/file/du_lieu_input.xlsx'
    filename = f'du_lieu_input.xlsx'
    if 'xlsx' in filename:
        df = pd.read_excel(
            file_path, sheet_name='Danh mục thông tin phí phúc lợi')
    df.drop_duplicates()
    ti.xcom_push('extract_data', df.to_json(orient='records'))

# [END extract_function]
# [START transform_function]


def transform(ti):
    print("START Tranforms")
    transform_data = ProcessingData(DB_TYPE, CONNECTION)
    transform_data.transform(ti, task_ids='extract', key='extract_data')

# [END transform_function]
# [START load_function]


def load(ti):
    print("START LOAD TO DES DB")
    # truncate use class
    connection_helper = ProcessingData(DB_TYPE, CONNECTION)
    connection_helper.truncate(table_name=TABLE_NAME, schema_name=SCHEMA_NAME)
    connection_helper.insert_data(ti, TABLE_NAME, task_ids="transform", key="transform_data")


def check_data_imported():
    connection_helper = ProcessingData(DB_TYPE, CONNECTION)
    connection_helper.check_data(TABLE_NAME, SCHEMA_NAME)


# [END load_function]
# [START instantiate_dag]
with DAG(
    'etl_stg_test_cm',
    default_args={'retries': 2},
    description='ETL_STG_DANH_MUC_TT_PHI_PHUC_LOI',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl', 'import', 'stg', 'test'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\#### Extract task
        Extract data from source catalog table, unique value"""
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\#### Transform task
        Add identity column for data"""
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """#### Load task
        Import data to dwh database"""
    )

    check_import_task = PythonOperator(
        task_id='check_import',
        python_callable=check_data_imported
    )
    check_import_task.doc_md = dedent(
        """#### Check data import
        Print total data imported !"""
    )

    extract_task >> transform_task >> load_task >> check_import_task

# [END main_flow]
# [END tutorial]
