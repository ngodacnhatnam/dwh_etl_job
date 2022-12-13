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
    'etl_fact_tinhtrangsudungdat',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Import fact tinh trang su dung dat',
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
        INSERT INTO "DWH"."FACT_TINH_TRANG_SU_DUNG_DAT" ("NGAY_DL","ID_LOAI_DAT", "ID_DON_VI", "DIEN_TICH" ,"NAM_BAO_CAO")
        SELECT to_char(current_date,'YYYYMMDD')::INT,b."ID_LOAI_DAT"::int
        ,c."ID_DON_VI"::INT,  a."DIEN_TICH" , a."NAM_BAO_CAO"
        FROM "STG"."STG_TINH_TRANG_SU_DUNG_DAT" a
        LEFT JOIN "DWH"."DIM_PHAN_LOAI_DAT"  b ON a."MA_LOAI_DAT" = b."MA_LOAI_DAT_CAP3"
        LEFT JOIN "DWH"."DIM_DON_VI_HANH_CHINH" c  ON a."MA_DON_VI_SD" = c."MA_DON_VI"
        """
        engine.execute(query)

    def delete():
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """    
            DELETE FROM "DWH"."FACT_TINH_TRANG_SU_DUNG_DAT" WHERE "NGAY_DL" = to_char(current_date,'YYYYMMDD')::INT
        """
        engine.execute(query)
        return {"table FACT_TINH_TRANG_SU_DUNG_DAT ": "Data imported successful"}
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