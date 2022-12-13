# [END main_flow]

# [END tutorial]


import time
import pandas as pd
import datetime
from sqlalchemy import create_engine
import sqlalchemy
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import uuid
import requests
# [START tutorial]
# [START import_module]
import json
from textwrap import dedent
from requests.auth import HTTPBasicAuth

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import re

"""
### ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

SCHEMA = "DWH"
username="ngoan"
password = "123456"
CONSTANT_DATE_IMPORT = '2021-06-28'
TABLE_NAME = "DIM_SAN_PHAM"
# [END import_module]

# [START instantiate_dag]
with DAG(
    'etl_fact_tinhtrangkhaikhackhoangsan',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Import fact khai thac khoang san',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 8, 1, tz="UTC"),
    catchup=False,
    tags=['etl','fact', 'daklak'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    


    def insert():
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
        INSERT INTO "DWH"."FACT_TINH_TRANG_KT_KHOANG_SAN" ("NGAY_DL","ID_KHOANG_SAN", "TONG_TRU_LUONG", "TL_DA_KHAI_THAC" ,"NAM_BAO_CAO")
        SELECT to_char(current_date,'YYYYMMDD')::INT,b."ID_KHOANG_SAN"::int,a."TONG_TRU_LUONG"::INT,  a."TL_DA_KHAI_THAC"  , a."NAM_BAO_CAO"
        FROM "STG"."STG_TINH_TRANG_KT_KHOANG_SAN"  a
        LEFT JOIN "DWH"."DIM_LOAI_KHOANG_SAN"  b ON a."MA_KHOANG_SAN" = b."MA_KHOANG_SAN"
        """
        engine.execute(query)

    def delete():
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """    
            DELETE FROM "DWH"."FACT_TINH_TRANG_KT_KHOANG_SAN" WHERE "NGAY_DL" = to_char(current_date,'YYYYMMDD')::INT
        """
        engine.execute(query)
        return {"table FACT_TINH_TRANG_KT_KHOANG_SAN ": "Data imported successful"}
    # [END transform_function]

        

    # [START main_flow]
    insert_task = PythonOperator(
        task_id='insert_task',
        python_callable=insert
    )
    delete_task = PythonOperator(
        task_id='delete_task',
        python_callable=delete
    )
    
    delete_task >> insert_task

# [END main_flow]

# [END tutorial]