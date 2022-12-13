
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
from minio import Minio

# Operators; we need this to operate!
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import timedelta

"""
### ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""


# [END import_module]

# [START instantiate_dag]
with DAG(
    'etl_stg_tieuchi_chondiemden',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Import tiêu chí chọn điểm đến',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl','stg','import'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START extract_function]

    def extract(**kwargs):
        filename = f'DemoBI.xlsx'
        client = Minio('172.16.50.81:9000',
               access_key='meTNi42J77H0Coyu',
               secret_key='TrtbMicYwLhaiepGTDVUBPU8gpEwKEWi',
              secure=False)
        file_path = client.presigned_get_object('demodwh',
                                       'DemoBI.xlsx',
                                       expires=timedelta(minutes=5))
        if 'xlsx' in filename:
            df = pd.read_excel(file_path, sheet_name=f'TIEU_CHI_LUA_CHON_DIEM_DEN', header=0, names=['LOAI_TIEU_CHI','TY_LE'])
        ti = kwargs['ti']
        df.drop_duplicates()
        ti.xcom_push('canbo', df.to_json(orient='records'))

    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        print("START Tranforms")
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract', key='canbo')
        df1 = pd.read_json(data)
        df = df1.copy()
        # df['uuid'] = [str(uuid.uuid4()) for _ in range(len(df.index))]
        ti.xcom_push('transform_data', df.to_json(orient='records'))

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        print("START LOAD TO DES DB")
        # trancate dữ liệu
        hook = PostgresHook(postgres_conn_id="dwh_etl")
        conn = hook.get_conn()
        cursor = conn.cursor()
        delete_sql_query = """
        TRUNCATE TABLE  "STG"."STG_TIEU_CHI_CHON_DIEM_DEN";
        """
        cursor.execute(delete_sql_query)
        conn.commit()
        # đẩy dữ liệu vào bảng
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='transform', key='transform_data')
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        df1 = pd.read_json(data)
        df = df1.copy()
        df.to_sql('STG_TIEU_CHI_CHON_DIEM_DEN', con=engine, schema='STG', if_exists='replace',index=False)
        return {"table STG_TIEU_CHI_CHON_DIEM_DEN ": "Data imported successful"}

    def check_data_imported():
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = 'select * from "STG"."STG_TIEU_CHI_CHON_DIEM_DEN"'
        df = pd.read_sql(query, con=engine)
        print(f"FOUND : {len(df)} record location imported !")
        return {"status ": "Success !"}
    # [END load_function]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    Extract data from source catalog table, unique value
    """
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    Add identity column for data
    """
    )

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
    extract_task >> transform_task >> load_task >> check_import_task

# [END main_flow]

# [END tutorial]