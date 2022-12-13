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
    'daklak_fact_tinh_trang_kttn_nuoc',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Fact Hình thức khai thác tài nguyên nước',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 8, 1, tz="UTC"),
    catchup=False,
    tags=['etl','fact','daklak'],
) as dag:

    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    # # [START load_function]

    def truncate(**kwargs):
        print("START LOAD TO DES DB")
        # thêm mới dữ liệu từ bảng staging in dim
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        
        query = '''
        Truncate table "DWH"."FACT_TINH_TRANG_KTTN_NUOC"

        '''
        engine.execute(query)
        return {"msg": "OK"}
    # [END load_function]

    def insert():
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query ="""
        INSERT INTO "DWH"."FACT_TINH_TRANG_KTTN_NUOC"
        SELECT B."ID_HINH_THUC_KT" ,TO_CHAR(current_date ,'YYYYMMDD')::int  as "NGAY_DL", A."SLGP_DA_CAP" , A."TL_NUOC_DA_CAP_PHEP_KT" , A."SLGP_DA_CAP_LUY_KE" , A."TLNUOC_DA_CAP_PHEP_KT_LK" , A."NAM_BAO_CAO" 
        FROM "STG"."STG_TINH_TRANG_KTTN_NUOC"   as A 
        LEFT JOIN "DWH"."DIM_HINH_THUC_KTTN_NUOC" as B  on lower( B."TEN_HINH_THUC_KT")=LOWER(A."TEN_HINH_THUC_KT")
        """
        engine.execute(query)
        return {"msg": "OK"}

    truncate_task = PythonOperator(
        task_id='truncate',
        python_callable=truncate,
    )
    insert_task = PythonOperator(
        task_id='insert',
        python_callable=insert,
    )

    truncate_task >> insert_task

# [END main_flow]

# [END tutorial]