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
    'daklak_etl_dim_hinh_thuc_kttn_nuoc',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG DIM Hình thức khai thác tài nguyên nước',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 8, 1, tz="UTC"),
    catchup=False,
    tags=['etl','dim','daklak'],
) as dag:

    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    # # [START load_function]

    def insert(**kwargs):
        print("START LOAD TO DES DB")
        # thêm mới dữ liệu từ bảng staging in dim
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        
        insert_sql_query = '''insert into "DWH"."DIM_HINH_THUC_KTTN_NUOC"("MA_HINH_THUC_KT","TEN_HINH_THUC_KT","TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
        select a."MA_HINH_THUC_KT", a."TEN_HINH_THUC_KT", 'A', current_date, '2099-01-01'::DATE
        from "STG"."STG_HINH_THUC_KTTN_NUOC" as a
        where a."MA_HINH_THUC_KT" not in (
            select "MA_HINH_THUC_KT" from "DWH"."DIM_HINH_THUC_KTTN_NUOC" 	
        );'''
        engine.execute(insert_sql_query)
        return {"msg": "OK"}
    # [END load_function]

    def reinsert():
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query ="""
        INSERT INTO "DWH"."DIM_HINH_THUC_KTTN_NUOC"("MA_HINH_THUC_KT","TEN_HINH_THUC_KT","TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
        select a."MA_HINH_THUC_KT", a."TEN_HINH_THUC_KT", 'A', current_date, '2099-01-01'::DATE
        from "STG"."STG_HINH_THUC_KTTN_NUOC" as a
        where a."MA_HINH_THUC_KT" not in (
            select "MA_HINH_THUC_KT" from "DWH"."DIM_HINH_THUC_KTTN_NUOC" where "TRANG_THAI_BG" ='A'
        )
        """
        engine.execute(query)
        return {"msg": "OK"}


    def update(**kwargs):
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
        UPDATE "DWH"."DIM_HINH_THUC_KTTN_NUOC" A
        SET "TRANG_THAI_BG" = 'C', "NGAY_HH_BG" = current_date 
        from "STG"."STG_HINH_THUC_KTTN_NUOC" B
        where  A."NGAY_HL_BG" <>current_date  and  (
            A."MA_HINH_THUC_KT"  = B."MA_HINH_THUC_KT" 
            or A."TEN_HINH_THUC_KT" <>B."TEN_HINH_THUC_KT"
        )
        """
        engine.execute(query)
        return {"msg": "OK"}

    def delete():
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
        UPDATE "DWH"."DIM_HINH_THUC_KTTN_NUOC" A
        SET "TRANG_THAI_BG" = 'C', "NGAY_HH_BG" = current_date 
        WHERE A."MA_HINH_THUC_KT" NOT IN (
            SELECT "MA_HINH_THUC_KT" FROM "STG"."STG_HINH_THUC_KTTN_NUOC"
        ) AND A."TRANG_THAI_BG" ='A'
        """
        engine.execute(query)


    insert_task = PythonOperator(
        task_id='insert',
        python_callable=insert,
    )
    update_task = PythonOperator(
        task_id='update',
        python_callable=update,
    )
    reinsert_task = PythonOperator(
        task_id='reinsert',
        python_callable=reinsert,
    )
    delete_task = PythonOperator(
        task_id='delete',
        python_callable=delete
    )

    insert_task >> update_task >> reinsert_task >> delete_task

# [END main_flow]

# [END tutorial]