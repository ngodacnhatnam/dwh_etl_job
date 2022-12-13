
import time
import pandas as pd
from pendulum import datetime
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
from datetime import timedelta
from airflow.models import Variable
import uuid

# [START tutorial]
# [START import_module]
import json
from textwrap import dedent

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from minio import Minio
from airflow.operators.python import PythonOperator

"""
### ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

TARGET_TABLE_NAME = "STG_HINH_THUC_KTTN_NUOC"
MINIO_ENDPOINT = Variable.get("MIN_IO_ENDPOINT")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")


# [END import_module]

# [START instantiate_dag]
with DAG(
    'daklak_stg_hinh_thuc_kttn_nuoc',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Import hình thức khai thác tài nguyên nước qua các năm',
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
        client = Minio(
                MINIO_ENDPOINT,
               access_key=MINIO_ACCESS_KEY,
               secret_key=MINIO_SECRET_KEY,
              secure=False)
        download_url = client.presigned_get_object('demodwh',
                                       'daklak_data/STG_HINH_THUC_KTTN_NUOC.xlsx',
                                       expires=timedelta(minutes=5))
        df = pd.read_excel(download_url)
        ti = kwargs['ti']
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        df.to_sql(f'temp_{TARGET_TABLE_NAME}', con=engine, schema='STG', if_exists='replace',index=False)
        ti.xcom_push('extract_data', df.to_json(orient='records'))

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        conn = BaseHook.get_connection('daklak_dwh')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        # truncate table
        delete_sql_query = f"""
        TRUNCATE TABLE  "STG"."{TARGET_TABLE_NAME}";
        """
        engine.execute(delete_sql_query)
        # đẩy dữ liệu vào bảng
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract', key='extract_data')
        
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