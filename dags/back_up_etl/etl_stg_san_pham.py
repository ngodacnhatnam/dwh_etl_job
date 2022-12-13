
import time
import pandas as pd
import datetime
from sqlalchemy import create_engine
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

"""
### ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

SCHEMA = "STG"
username="ngoan"
password = "123456"
CONSTANT_DATE_IMPORT = '2021-06-28'
# [END import_module]

# [START instantiate_dag]
with DAG(
    'etl_san_pham_import',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Import SAN PHAM',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 8, 1, tz="UTC"),
    catchup=False,
    tags=['etl','import'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]


    def generate_columns(df):
        df1 = df.copy()
        columns = [{
            "product_name": "TEN_SAN_PHAM",
            "brand_name": "TEN_HANG_SX",
            "ingredient": "CHAT_LIEU",
            "model_num": "SO_MODEL",
            "colors": "MAU_SAC",
            "available": "TRANG_THAI_SP",
            "dimensions": "KICH_THUOC",
            "unit": "DON_VI",
            "original_price": "GIA_SAN_PHAM"
        }]
        for column in columns:
            for k,v in column.items():
                df1[v] = df1['data'].map(lambda x: x.get(k))
        return df1
    
    def get_date_import():
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
        select max(date_import) as max_date from "{}".date_import
        """.format(SCHEMA)
        rs = engine.execute(query)
        date_import = None
        for row in rs:
            date_import = row['max_date']
            if isinstance(date_import, str):
                date_import = datetime.datetime.strptime(date_import, "%Y-%m-%d")
        if not date_import:
            date_import = datetime.datetime.strptime(CONSTANT_DATE_IMPORT, "%Y-%m-%d")
        next_date_import_timestamp = date_import + datetime.timedelta(days=1)
        date_time_str = next_date_import_timestamp.strftime('%Y-%m-%d')
        timestamp = datetime.datetime.strptime(date_time_str, '%Y-%m-%d')
        return {
            "timestamp": timestamp,
            "date_string": date_import.strftime('%Y-%m-%d')
        }



    def trigger(sdate):
        url = 'http://localhost:8080/api/v1/dags/etl_san_pham_import/dagRuns'
        headers = {
             'Content-Type' : "application/json",
              "Cache-Control": "no-cache"
              }
        utc_dt_aware = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            "conf": {"sdate": sdate},
            "dag_run_id": generate_dag_run_id(),
            "logical_date": str(utc_dt_aware)
        }
        r = requests.post(url, json=payload, headers=headers, auth=HTTPBasicAuth(username, password))
        print("*****************", r.request.headers['Authorization'])
        print(r.json())

    def generate_dag_run_id():
        today = datetime.datetime.now(datetime.timezone.utc)
        today_str = str(today)
        return "trigger_via_restapi__{}".format(today_str)
    
    def insert_to_date_import():
        sdate = get_date_import()
        timestamp = sdate['timestamp']
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
        insert into "{}".date_import(date_import, import_at) values(%s, now())
        """.format(SCHEMA)
        try:
            engine.execute(query,(timestamp))
        except Exception as ex:
            print("===============",ex)
    
    def get_min_date_from_source(from_date: str):
        query = "select min(created_at) as min_date from catalog where created_at::date > %s"
        conn = BaseHook.get_connection('spider_postgres_db')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        rs = engine.execute(query,(from_date))
        min_date = None
        for row in rs:
            min_date = row['min_date']
            if isinstance(min_date, str):
                min_date = datetime.datetime.strptime(min_date, "%Y-%m-%d")
        return min_date.strftime('%Y-%m-%d')

    def merge_data():
        query = f"""
            INSERT INTO "{SCHEMA}"."STG_SAN_PHAM"
            SELECT *
            FROM (
                SELECT   *
                FROM "{SCHEMA}".temp_product
            ) as foo
            ON CONFLICT (ma_san_pham) DO UPDATE
            SET TEN_SAN_PHAM = excluded.TEN_SAN_PHAM,
            TEN_HANG_SX = excluded.TEN_HANG_SX,
            CHAT_LIEU = excluded.CHAT_LIEU,
            SO_MODEL = excluded.SO_MODEL,
            MAU_SAC = excluded.MAU_SAC,
            TRANG_THAI_SP = excluded.TRANG_THAI_SP,
            KICH_THUOC = excluded.KICH_THUOC,
            DON_VI = excluded.DON_VI,
            NGAY_CAP_NHAT = excluded.NGAY_CAP_NHAT,
            GIA_SAN_PHAM = excluded.GIA_SAN_PHAM,
            MA_NHOM_SP = excluded.MA_NHOM_SP,
            MA_NCC = excluded.MA_NCC;
            
        """
        print(query)
        try:
            conn = BaseHook.get_connection('dwh_postgres')
            engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            engine.execute(query)
            return 0
        except Exception as e:
            print("****", e)
            return 1

    # [START extract_function]
    def extract(**kwargs):
        ti = kwargs['ti']
        conn = BaseHook.get_connection('spider_postgres_db')
        sdate = get_date_import()
        sdate_string = sdate['date_string']
        if not sdate:
            print("not found params `sdate`")
            today =  datetime.datetime.now()
            today_str = today.strftime('%Y-%m-%d')
            sdate_string = today_str
        print('Extract data with date : ', sdate_string)
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
            select * from catalog  
            where
            created_at::date = %(sdate)s
        """
        df = pd.read_sql(query, con=engine, params={'sdate': sdate_string})
        print(f"FOUND {len(df)}  records !")
        df['MA_SAN_PHAM'] = df['id']
        df['MA_NHOM_SP'] = df['category_id']
        df['MA_NCC'] = df['website_id']
        df['NGAY_CAP_NHAT'] = df['created_at'].map(lambda x : str(x))
        df2 = generate_columns(df)
        columns = [
            "MA_SAN_PHAM", "TEN_SAN_PHAM", "TEN_HANG_SX","CHAT_LIEU","SO_MODEL","MAU_SAC","TRANG_THAI_SP","KICH_THUOC","DON_VI","NGAY_CAP_NHAT","GIA_SAN_PHAM","MA_NHOM_SP","MA_NCC"
        ]
        df22 = df2.fillna('')
        df3 =df22[columns]
        ti.xcom_push('extract_data', df3.to_json(orient='records'))

    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        print("START Tranforms")
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract', key='extract_data')
        df1 = pd.read_json(data)
        df = df1.copy()
        ti.xcom_push('transform_data', df.to_json(orient='records'))

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        print("START LOAD TO DES DB")
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='transform', key='transform_data')
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        df1 = pd.read_json(data)
        df = df1.copy()

        df.to_sql('temp_product', con=engine, schema=SCHEMA, if_exists='replace', index=False)

        return {"table STG_SAN_PHAM ": "Data imported successful"}

    def run_next_date(**kwargs):
        sdate = get_date_import()
        sdate_string = sdate['date_string']
        current_date = datetime.datetime.strptime(sdate_string, "%Y-%m-%d")
        next_date = current_date + datetime.timedelta(days=1)
        next_date_str = next_date.strftime('%Y-%m-%d')
        # taget_date
        taget_date = datetime.datetime.strptime('2022-06-11', '%Y-%m-%d')
        if next_date < taget_date:
            print("Trigger with date ", next_date)
            trigger(sdate=next_date_str)

    # [START main_flow]
    create_date_import_table_task =  PostgresOperator(
        task_id="create_date_import_table",
        postgres_conn_id="dwh_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS "STG".date_import (
                date_import timestamp,
                import_at timestamp
            );""",
    )
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    Extract data from source brand table
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
    merge_task = PythonOperator(
        task_id='merge_task',
        python_callable=merge_data
    )
    run_next_date_task = PythonOperator(
        task_id='run_next_date',
        python_callable= run_next_date
    )
    insert_to_date_import_task = PythonOperator(
        task_id="insert_to_date_import_task",
        python_callable=insert_to_date_import
    )
    create_date_import_table_task >> extract_task >> transform_task >> load_task >> merge_task >> insert_to_date_import_task >> run_next_date_task

# [END main_flow]

# [END tutorial]