
import time
import pandas as pd
from pendulum import datetime
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
import uuid
import pandas as pd

# [START tutorial]
# [START import_module]
import json
from textwrap import dedent

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

"""
### ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""


# [END import_module]

# [START instantiate_dag]
with DAG(
    'etl_vungmien_dim',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG DIM vung mien',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dim'],
) as dag:

    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    # # [START load_function]

    def load(**kwargs):
        print("START LOAD TO DES DB")
        # thêm mới dữ liệu từ bảng staging in dim 
        hook = PostgresHook(postgres_conn_id="dwh_etl")
        df = hook.get_pandas_df(sql='''SELECT * FROM "STG"."STG_VUNG_MIEN"''')
        
        insert_sql_query = '''INSERT INTO "DWH"."DIM_VUNG_MIEN"("MA_VUNG_MIEN", "TEN_VUNG_MIEN","TEN_TINH_THANH")
        SELECT D."MA_VUNG_MIEN", D."TEN_VUNG_MIEN",D."TEN_TINH_THANH"
        FROM "STG"."STG_VUNG_MIEN"  D 
        WHERE NOT EXISTS (SELECT 'X'
                         FROM "DWH"."DIM_VUNG_MIEN" E
                   WHERE E."TEN_TINH_THANH" = D."TEN_TINH_THANH")  AND D."TEN_TINH_THANH" IS NOT NULL
        ORDER BY D."TEN_TINH_THANH";'''
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(insert_sql_query)
        conn.commit()
        
        update_sql_query = """
        UPDATE "DWH"."DIM_VUNG_MIEN" A
        SET "MA_VUNG_MIEN" = D."MA_VUNG_MIEN", "TEN_VUNG_MIEN" = D."TEN_VUNG_MIEN"
        FROM "STG"."STG_VUNG_MIEN" D
        WHERE A."TEN_TINH_THANH" = D."TEN_TINH_THANH" AND D."TEN_TINH_THANH" IS NOT NULL;
        """
        cursor.execute(update_sql_query)
        conn.commit()
        return {"table STG_VUNG_MIEN ": "Data imported successful"}
    # [END load_function]

    # [START check_data]
    def check_data_imported():
        hook = PostgresHook(postgres_conn_id="dwh_etl")
        df = hook.get_pandas_df(sql='SELECT * FROM "DWH"."DIM_VUNG_MIEN"')
        return {"status ": "Success !"}
    # [END check_data]

    select_date_stg = PythonOperator(
        task_id='select_stg',
        python_callable=load,
    )
    select_date_stg.doc_md = dedent(
        """\
    #### Extract task
    Extract data from source brand table
    """
    )

    insert_data_dim = PythonOperator(
        task_id='insert_data',
        python_callable=load,
    )
    insert_data_dim.doc_md = dedent(
        """\
    #### Transform task
    Add identity column for data
    """
    )

    update_data_dim = PythonOperator(
        task_id='update_data',
        python_callable=load,
    )
    update_data_dim.doc_md = dedent(
        """\
    #### Load task
    Import data to dwh database
    """
    )
    select_dim = PythonOperator(
        task_id='select_dim',
        python_callable=check_data_imported
    )
    select_dim.doc_md=dedent(
        """
        #### Check data import
        Print total data imported !
        """
    )

    select_date_stg >> insert_data_dim >> update_data_dim >> select_dim

# [END main_flow]

# [END tutorial]