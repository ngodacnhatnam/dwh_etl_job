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
    'etl_fact_th_sl_sp',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG Import fact sản phẩm',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 8, 1, tz="UTC"),
    catchup=False,
    tags=['etl','fact'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    
    def get_date_import():
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
        select max(date_import) as max_date from "DWH".date_import_fact_th_sl_sp
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
        url = 'http://localhost:8080/api/v1/dags/etl_fact_th_sl_sp/dagRuns'
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
        insert into "DWH".date_import_fact_th_sl_sp(date_import, import_at) values(%s, now())
        """
        try:
            engine.execute(query,(timestamp))
        except Exception as ex:
            print("===============",ex)
    
    def get_min_date_from_source(from_date: str):
        query = """
        select min("NGAY_CAP_NHAT") as min_date from "DWH"."DIM_SAN_PHAM" where "NGAY_CAP_NHAT"::date > %s
        """
        conn = BaseHook.get_connection('spider_postgres_db')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        rs = engine.execute(query,(from_date))
        min_date = None
        for row in rs:
            min_date = row['min_date']
            if isinstance(min_date, str):
                min_date = datetime.datetime.strptime(min_date, "%Y-%m-%d")
        return min_date.strftime('%Y-%m-%d')

    def insert():
        sdate = get_date_import()
        sdate_string = sdate['date_string']
        if not sdate:
            print("not found params `sdate`")
            today =  datetime.datetime.now()
            today_str = today.strftime('%Y-%m-%d')
            sdate_string = today_str
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        query = """
        INSERT INTO "DWH"."FACT_TONG_HOP_SL_SP" ("NGAY_DL","ID_VUNG_MIEN", "ID_NHOM_SP","ID_NHA_CC", "ID_HANG_SX" ,"SO_LUONG_SP"
        )
        SELECT to_char(cc."NGAY_CAP_NHAT",'YYYYMMDD')::INT, (case when TRIM(UPPER(a."TEN_TINH_THANH")) = 'HCM' then (select "ID_VUNG_MIEN" FROM "DWH"."DIM_VUNG_MIEN" WHERE "TEN_TINH_THANH" = 'TP Hồ Chí Minh')
        when TRIM(UPPER(a."TEN_TINH_THANH")) = 'HN' then (select "ID_VUNG_MIEN" FROM "DWH"."DIM_VUNG_MIEN" WHERE "TEN_TINH_THANH" = 'Hà Nội')
        when TRIM(UPPER(a."TEN_TINH_THANH")) = 'Huế' then (select "ID_VUNG_MIEN" FROM "DWH"."DIM_VUNG_MIEN" WHERE "TEN_TINH_THANH" = 'Thừa Thiên Huế')
        when a."TEN_TINH_THANH" is null or TRIM(UPPER(a."TEN_TINH_THANH")) != '' THEN NULL
		 else (select "ID_VUNG_MIEN" FROM "DWH"."DIM_VUNG_MIEN" WHERE TRIM(UPPER(a."TEN_TINH_THANH")) = TRIM(UPPER("TEN_TINH_THANH")))
        end)::int AS "ID_VUNG_MIEN",  nsp."ID_NHOM_SP"::INT,a."ID_NHA_CC"::INT, hsx."ID_HANG_SX"::INT, count(cc."MA_SAN_PHAM")::INT
        FROM "DWH"."DIM_SAN_PHAM" cc 
        LEFT JOIN "DWH"."DIM_NHA_CC" a ON cc."MA_NCC" = a."MA_NHA_CC"
        LEFT JOIN "DWH"."DIM_NHOM_SP" nsp ON cc."MA_NHOM_SP" = nsp."MA_NHOM_SP"
        LEFT JOIN "DWH"."DIM_HANG_SX" hsx ON cc."TEN_HANG_SX" = hsx."TEN_HANG_SX"
        WHERE cc."NGAY_CAP_NHAT" = TO_DATE(%s,'YYYY-MM-DD')
        GROUP BY cc."NGAY_CAP_NHAT", a."ID_NHA_CC", "ID_VUNG_MIEN",  nsp."ID_NHOM_SP", hsx."ID_HANG_SX"
        """
        engine.execute(query,(sdate_string))

    def delete():
        sdate = get_date_import()
        sdate_string = sdate['date_string']
        if not sdate:
            print("not found params `sdate`")
            today =  datetime.datetime.now()
            today_str = today.strftime('%Y%m%d')
            sdate_string = today_str
        else:
            sdate_obj = datetime.datetime.strptime(sdate_string, '%Y-%m-%d')
            sdate_string = sdate_obj.strftime('%Y%m%d')
        conn = BaseHook.get_connection('dwh_postgres')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        # ,"TEN_SAN_PHAM","TEN_HANG_SX","CHAT_LIEU","SO_MODEL","MAU_SAC","TRANG_THAI_SP","KICH_THUOC","DON_VI","NGAY_CAP_NHAT","GIA_SAN_PHAM","MA_NHOM_SP","MA_NCC"  from "DWH"."DIM_SAN_PHAM"
        query = """    
            DELETE FROM "DWH"."FACT_TONG_HOP_SL_SP" WHERE TO_CHAR(TO_DATE('20200710', 'YYYYMMDD'), 'YYYY-MM-DD')= %s
        """
        engine.execute(query,(sdate_string))
        
        

    # [END transform_function]

        return {"table STG_SAN_PHAM ": "Data imported successful"}

    def run_next_date(**kwargs):
        sdate = get_date_import()
        sdate_string = sdate['date_string']
        current_date = datetime.datetime.strptime(sdate_string, "%Y-%m-%d")
        next_date = current_date + datetime.timedelta(days=1)
        next_date_str = next_date.strftime('%Y-%m-%d')
        # taget_date
        taget_date = datetime.datetime.strptime('2022-06-12', '%Y-%m-%d')
        if next_date < taget_date:
            print("Trigger with date ", next_date)
            trigger(sdate=next_date_str)

    # [START main_flow]
    create_date_import_table_task =  PostgresOperator(
        task_id="create_date_import_table",
        postgres_conn_id="dwh_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS "DWH".date_import_fact_th_sl_sp (
                date_import timestamp,
                import_at timestamp
            );""",
    )
    insert_task = PythonOperator(
        task_id='insert_task',
        python_callable=insert
    )
    delete_task = PythonOperator(
        task_id='delete_task',
        python_callable=delete
    )
    

    run_next_date_task = PythonOperator(
        task_id='run_next_date',
        python_callable= run_next_date
    )
    insert_to_date_import_task = PythonOperator(
        task_id="insert_to_date_import_task",
        python_callable=insert_to_date_import
    )
    create_date_import_table_task  >> delete_task >> insert_task >> run_next_date_task >> insert_to_date_import_task

# [END main_flow]

# [END tutorial]