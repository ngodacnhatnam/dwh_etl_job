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
TABLE_NAME = "FACT_SO_LIEU_DU_LIEU_TINH"
# [END import_module]

# [START instantiate_dag]
with DAG(
    'etl_fact_solieudulieutinh',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Import fact so lieu du lieu tinh',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 8, 1, tz="UTC"),
    catchup=False,
    tags=['etl','fact'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    


    def insert():
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
        INSERT INTO "DWH"."FACT_SO_LIEU_DU_LIEU_TINH" ("NGAY_DL","ID_PHAN_LOAI_KHACH", "SO_LUONG", "NAM")
       SELECT to_char(current_date,'YYYYMMDD')::INT,b."ID_LOAI_KHACH"::int
        ,a."SO_LUONG"::INT,  REGEXP_REPLACE(a."NAM",'[[:alpha:]^\W\s]','','g')::int
       	FROM "STG"."STG_SO_LIEU_DL_TINH" a
        LEFT JOIN "DWH"."DIM_PHAN_LOAI_KHACH" b  ON a."PHAN_LOAI" = b."TEN_LOAI_KHACH" 
        """
        engine.execute(query)

    def delete():
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """    
            DELETE FROM "DWH"."FACT_SO_LIEU_DU_LIEU_TINH" WHERE "NGAY_DL" = to_char(current_date,'YYYYMMDD')::INT
        """
        engine.execute(query)
        return {"table FACT_SO_LIEU_DU_LIEU_TINH ": "Data imported successful"}
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