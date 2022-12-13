
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
    'etl_dim_san_pham',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Import SAN PHAM',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 8, 1, tz="UTC"),
    catchup=False,
    tags=['etl','dim'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    def get_avg_price_by_category_id(cate_id):
        query = """
        select min("GIA_SAN_PHAM"), max("GIA_SAN_PHAM") from "DWH"."DIM_SAN_PHAM" 
        where "GIA_SAN_PHAM" >=100000 and "MA_NHOM_SP" =%s
        """
        rows = engine.execute(query,(cate_id))
        for row in rows:
            return {
                "min": row['min'],
                'max': row['max']
            }
        


    def normalized_price(price_str):
        all_numbers= re.findall('[0-9]+', price_str)
        result_str = ''
        result_num = 0
        excepts = [
            "liên hệ", "lh","vui lòng gọi"
        ]
        if price_str in excepts:
            return -1
        for num in all_numbers:
            result_str+=str(num)
        if len(result_str) >= 9:
            return -1
        return 0 if result_str =='' else int(result_str)

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
            "updated_at": "NGAY_CAP_NHAT",
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
        select max(date_import) as max_date from "DWH".date_import
        """
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
        insert into "DWH".date_import(date_import, import_at) values(%s, now())
        """
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
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query_insert = """
           insert into "DWH"."DIM_SAN_PHAM"("MA_SAN_PHAM","TEN_SAN_PHAM","TEN_HANG_SX","CHAT_LIEU","SO_MODEL","MAU_SAC","TRANG_THAI_SP",
           "KICH_THUOC","DON_VI","NGAY_CAP_NHAT","GIA_SAN_PHAM","MA_NHOM_SP","MA_NCC","TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
            select "MA_SAN_PHAM","TEN_SAN_PHAM","TEN_HANG_SX","CHAT_LIEU","SO_MODEL","MAU_SAC","TRANG_THAI_SP","KICH_THUOC","DON_VI",
            TO_TIMESTAMP("NGAY_CAP_NHAT",'YYYY-MM-DD HH24:MI:SS'),"GIA_SAN_PHAM","MA_NHOM_SP","MA_NCC",'A' as "TRANG_THAI_BG", now() as "NGAY_HL_BG",
            TO_TIMESTAMP('2099-01-01','YYYY-MM-DD HH24:MI:SS')"NGAY_HH_BG" from "DWH".temp_product_dim 
            where "MA_SAN_PHAM" not in (select "MA_SAN_PHAM" from "DWH"."DIM_SAN_PHAM")
        """

    def insert():
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
           
            insert into "DWH"."DIM_SAN_PHAM"("MA_SAN_PHAM","TEN_SAN_PHAM","TEN_HANG_SX","CHAT_LIEU","SO_MODEL","MAU_SAC","TRANG_THAI_SP",
           "KICH_THUOC","DON_VI","NGAY_CAP_NHAT","GIA_SAN_PHAM","MA_NHOM_SP","MA_NCC","TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
            select "MA_SAN_PHAM","TEN_SAN_PHAM","TEN_HANG_SX","CHAT_LIEU","SO_MODEL","MAU_SAC","TRANG_THAI_SP","KICH_THUOC","DON_VI",
            TO_TIMESTAMP("NGAY_CAP_NHAT",'YYYY-MM-DD HH24:MI:SS'),"GIA_SAN_PHAM","MA_NHOM_SP","MA_NCC",'A' as "TRANG_THAI_BG", now() as "NGAY_HL_BG",
            TO_TIMESTAMP('2099-01-01','YYYY-MM-DD HH24:MI:SS')"NGAY_HH_BG" from "DWH".temp_product_dim 
            where "MA_SAN_PHAM" not in (select "MA_SAN_PHAM" from "DWH"."DIM_SAN_PHAM")
        """
        engine.execute(query)

    def delete():
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        # ,"TEN_SAN_PHAM","TEN_HANG_SX","CHAT_LIEU","SO_MODEL","MAU_SAC","TRANG_THAI_SP","KICH_THUOC","DON_VI","NGAY_CAP_NHAT","GIA_SAN_PHAM","MA_NHOM_SP","MA_NCC"  from "DWH"."DIM_SAN_PHAM"
        query = """    
            update "DWH"."DIM_SAN_PHAM" set "TRANG_THAI_BG" = 'C' , "NGAY_HH_BG" = now() where "MA_SAN_PHAM" in   (
            select "MA_SAN_PHAM"
            where "MA_SAN_PHAM"  not in (select ma_san_pham  from "STG"."STG_SAN_PHAM"))
        """
        engine.execute(query)

    def update():
        # -- update --> update trang thai ban ghi o DIM ve Close va ngay het hieu luc la ngay ETL sau do them moi ban ghi
        query ="""
        update "DWH"."DIM_SAN_PHAM" set "TRANG_THAI_BG" = 'C' , "NGAY_HH_BG" = now()  where "MA_SAN_PHAM" in   (
            select A."MA_SAN_PHAM"  from "DWH".temp_product_dim  A, "DWH"."DIM_SAN_PHAM" B
            where A."MA_SAN_PHAM" = B."MA_SAN_PHAM"
            and( A."TEN_SAN_PHAM" <> B."TEN_SAN_PHAM" 
            or 
                A."TEN_HANG_SX" <> B."TEN_HANG_SX" 
                or A."CHAT_LIEU" <>B."CHAT_LIEU"
                or A."SO_MODEL" <> B."SO_MODEL"
                or A."MAU_SAC" <> B."MAU_SAC"
                or A."TRANG_THAI_SP" <> B."TRANG_THAI_SP"
                or A."KICH_THUOC" <> B."KICH_THUOC"
                or A."DON_VI" <> B."DON_VI"
                or A."GIA_SAN_PHAM" <> B."GIA_SAN_PHAM"
                or A."MA_NHOM_SP" <> B."MA_NHOM_SP"
                or A."MA_NCC" <> B."MA_NCC"
            ))
        """
        engine.execute(query)
        query_insert ="""
         insert into "DWH"."DIM_SAN_PHAM"("MA_SAN_PHAM","TEN_SAN_PHAM","TEN_HANG_SX","CHAT_LIEU","SO_MODEL","MAU_SAC","TRANG_THAI_SP",
        "KICH_THUOC","DON_VI","NGAY_CAP_NHAT","GIA_SAN_PHAM","MA_NHOM_SP","MA_NCC","TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
        select A."MA_SAN_PHAM",A."TEN_SAN_PHAM",A."TEN_HANG_SX",A."CHAT_LIEU",A."SO_MODEL",A."MAU_SAC",A."TRANG_THAI_SP",
        A."KICH_THUOC",A."DON_VI",TO_TIMESTAMP(A."NGAY_CAP_NHAT",'YYYY-MM-DD HH24:MI:SS'),A."GIA_SAN_PHAM",A."MA_NHOM_SP",A."MA_NCC",
        'A' as "TRANG_THAI_BG", now() as "NGAY_HL_BG",TO_TIMESTAMP('2099-01-01','YYYY-MM-DD HH24:MI:SS')"NGAY_HH_BG" from "DWH".temp_product_dim  A, "DWH"."DIM_SAN_PHAM" B
        where A."MA_SAN_PHAM" = B."MA_SAN_PHAM"
        and( A."TEN_SAN_PHAM" <> B."TEN_SAN_PHAM" 
        or 
            A."TEN_HANG_SX" <> B."TEN_HANG_SX" 
            or A."CHAT_LIEU" <>B."CHAT_LIEU"
            or A."SO_MODEL" <> B."SO_MODEL"
            or A."MAU_SAC" <> B."MAU_SAC"
            or A."TRANG_THAI_SP" <> B."TRANG_THAI_SP"
            or A."KICH_THUOC" <> B."KICH_THUOC"
            or A."DON_VI" <> B."DON_VI"
            or A."GIA_SAN_PHAM" <> B."GIA_SAN_PHAM"
            or A."MA_NHOM_SP" <> B."MA_NHOM_SP"
            or A."MA_NCC" <> B."MA_NCC"
        )
        """
        engine.execute(query_insert)
        
        

    # [START extract_function]
    def extract(**kwargs):
        ti = kwargs['ti']
        conn = BaseHook.get_connection('dwh_postgres')
        print('Extract data with date : ')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
            select * from "STG"."STG_SAN_PHAM"
        """
        df = pd.read_sql(query, con=engine)
        df.columns = [str(x).upper() for x in df.columns]
        print(f"FOUND {len(df)}  records !")
        df22 = df.fillna('')
        df22['GIA_SAN_PHAM'] = df22['GIA_SAN_PHAM'].map(lambda x: normalized_price(price_str=x))
        df22.to_sql('temp_product_dim', con=engine, schema="DWH", if_exists='replace', index=False)
        ti.xcom_push('extract_data', [])

    # [END transform_function]

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

    def fill_invalid_price():
        query = """
           update "DWH"."DIM_SAN_PHAM" set "GIA_SAN_PHAM" = random()*(t."MAX_PRICE" - t."MIN_PRICE")+t."MIN_PRICE" from (
            select "MA_NHOM_SP" as "CATE_ID", max("GIA_SAN_PHAM") as "MAX_PRICE",min("GIA_SAN_PHAM") as "MIN_PRICE"
                        from "DWH"."DIM_SAN_PHAM" group by "MA_NHOM_SP"
            ) as t 
            where t."CATE_ID" = "MA_NHOM_SP" and "GIA_SAN_PHAM" < 100000
        """
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        engine.execute(query)




    # [START main_flow]
    # create_date_import_table_task =  PostgresOperator(
    #     task_id="create_date_import_table",
    #     postgres_conn_id="dwh_postgres",
    #     sql="""
    #         CREATE TABLE IF NOT EXISTS "DWH".date_import (
    #             date_import timestamp,
    #             import_at timestamp
    #         );""",
    # )
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
    insert_task = PythonOperator(
        task_id='insert_task',
        python_callable=insert
    )
    delete_task = PythonOperator(
        task_id='delete_task',
        python_callable=delete
    )
    upddate_task = PythonOperator(
        task_id='upddate_task',
        python_callable=update
    )
    
    fill_invalid_price_task = PythonOperator(
        task_id="fill_invalid_price_task",
        python_callable=fill_invalid_price
    )
    # run_next_date_task = PythonOperator(
    #     task_id='run_next_date',
    #     python_callable= run_next_date
    # )
    # insert_to_date_import_task = PythonOperator(
    #     task_id="insert_to_date_import_task",
    #     python_callable=insert_to_date_import
    # )
    extract_task >> insert_task >> delete_task >> upddate_task >> fill_invalid_price_task

# [END main_flow]

# [END tutorial]