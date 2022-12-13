
import time
import pandas as pd
from pendulum import datetime
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
from datetime import timedelta
import uuid

# [START tutorial]
# [START import_module]
import json
from textwrap import dedent

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from minio import Minio

# Operators; we need this to operate!
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

"""
### ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

TARGET_TABLE_NAME = "STG_TINH_TRANG_KTTN_NUOC"


# [END import_module]

# [START instantiate_dag]
with DAG(
    'daklak_stg_tinh_trang_kttn_nuoc',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Import Tình trạng khai thác tài nguyên nước qua các năm',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 8, 22, tz="UTC"),
    catchup=False,
    tags=['etl','stg','daklak'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START extract_function]

    def extract(**kwargs):
        client = Minio('172.16.50.81:9000',
               access_key='meTNi42J77H0Coyu',
               secret_key='TrtbMicYwLhaiepGTDVUBPU8gpEwKEWi',
              secure=False)
        download_url = client.presigned_get_object('demodwh',
                                       'daklak_data/STG_TINH_TRANG_KTTN_NUOC.xlsx',
                                       expires=timedelta(minutes=5))
        df = pd.read_excel(download_url)
        ti = kwargs['ti']
        # TẠO BẢNG TẠM
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        df.to_sql(f'temp_{TARGET_TABLE_NAME}', con=engine, schema='STG', if_exists='replace',index=False)
        ti.xcom_push('extract_data', df.to_json(orient='records'))

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        print("START LOAD TO DES DB")
        # trancate dữ liệu
        hook = PostgresHook(postgres_conn_id="dwh_etl")
        conn = hook.get_conn()
        cursor = conn.cursor()
        delete_sql_query = f"""
        TRUNCATE TABLE  "STG"."{TARGET_TABLE_NAME}";
        """
        cursor.execute(delete_sql_query)
        conn.commit()
        # đẩy dữ liệu vào bảng
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract', key='extract_data')
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        df1 = pd.read_json(data)
        df = df1.copy()

        query =f"""
        INSERT INTO "STG"."{TARGET_TABLE_NAME}" 
        SELECT * FROM "STG"."temp_{TARGET_TABLE_NAME}"
        """
        engine.execute(query)
        return {f"table {TARGET_TABLE_NAME} ": "Data imported successful"}

    
    def drop_temp():
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query =f"""
        DROP TABLE "STG"."temp_{TARGET_TABLE_NAME}"
        """
        engine.execute(query)


    def check_data_imported():
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = f'select * from "STG"."{TARGET_TABLE_NAME}"'
        df = pd.read_sql(query, con=engine)
        print(f"FOUND : {len(df)} record location imported !")
        return {"status ": "Success !"}
    # [END load_function]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    drop_temp_task = PythonOperator(
        task_id='drop_temp',
        python_callable=drop_temp,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    Extract data from source catalog table, unique value
    """
    )

    # transform_task = PythonOperator(
    #     task_id='transform',
    #     python_callable=transform,
    # )
    # transform_task.doc_md = dedent(
    #     """\
    # #### Transform task
    # Add identity column for data
    # """
    # )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    Import data to dwh database
    """
    )
    check_import_task = PythonOperator(
        task_id='check_import',
        python_callable=check_data_imported
    )
    check_import_task.doc_md=dedent(
        """
        #### Check data import
        Print total data imported !
        """
    )
    extract_task >> load_task >> drop_temp_task >> check_import_task

# [END main_flow]

# [END tutorial]