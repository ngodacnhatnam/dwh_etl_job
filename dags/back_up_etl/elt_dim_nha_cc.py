
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
    'etl_nha_cc_dim',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG DIM nhacc',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dim'],
) as dag:

    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    # # [START load_function]

    def load(**kwargs):
        print("START LOAD TO DES DB")
        # thêm mới dữ liệu từ bảng staging in dim 
        hook = PostgresHook(postgres_conn_id="dwh_etl")
        df = hook.get_pandas_df(sql='''SELECT * FROM "STG"."STG_VUNG_MIEN"''')
        
        insert_sql_query = '''INSERT INTO "DWH"."DIM_NHA_CC"("MA_NHA_CC","TEN_NHA_CC", "DIA_CHI", "PHONE", "EMAIL","MA_SO_THUE", "TEN_TINH_THANH", "TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
                            SELECT  TRIM(a.ma_nha_cc),   TRIM(a.ten_nha_cc),   TRIM(a.dia_chi),  TRIM(a.phone),  TRIM(a.email),  TRIM(a.ma_so_thue),  TRIM(a.ten_tinh_thanh), 'A', CURRENT_DATE, '2099-01-01'::DATE
                            FROM "STG"."STG_NHA_CC" a
                            WHERE a.ma_nha_cc not in (select a.ma_nha_cc from "DWH"."DIM_NHA_CC")
                                  and a.ten_nha_cc is not null and trim(a.ten_nha_cc) != ''
                            ORDER BY a.ma_nha_cc'''
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(insert_sql_query)
        conn.commit()
        # xóa dữ liệu nếu stg không có khỏi bảng dim, update dữ liệu về null 

        update_sql_query = """
        UPDATE "DWH"."DIM_NHA_CC" 
        SET "TRANG_THAI_BG" = 'C', "NGAY_HH_BG" = CURRENT_DATE
        WHERE "MA_NHA_CC" IN (
        select "MA_NHA_CC"
        from "DWH"."DIM_NHA_CC"
        where "MA_NHA_CC"  not in (select ma_nha_cc from "STG"."STG_NHA_CC"));

        UPDATE "DWH"."DIM_NHA_CC"  A
        SET "TEN_NHA_CC" = TRIM(B.ten_nha_cc), "TEN_TINH_THANH" = TRIM(B.ten_tinh_thanh), "DIA_CHI" = TRIM(B.dia_chi), "PHONE" = TRIM(B.phone), "EMAIL" = TRIM(B.email), "MA_SO_THUE" =  TRIM(B.ma_so_thue)
        FROM "STG"."STG_NHA_CC"  B
        WHERE A."MA_NHA_CC" = B.ma_nha_cc and B.ten_nha_cc is not null and TRIM(B.ten_nha_cc) != '' and 
            (A."TEN_NHA_CC" != TRIM(B.ten_nha_cc) OR A."TEN_TINH_THANH" = TRIM(B.ten_tinh_thanh) OR A."DIA_CHI" = TRIM(B.dia_chi) OR A."PHONE" = TRIM(B.phone) OR A."EMAIL" = TRIM(B.email)  OR A."MA_SO_THUE" =  TRIM(B.ma_so_thue));
        
        update "DWH"."DIM_NHA_CC" set  "TEN_TINH_THANH" = 'Bắc Giang' where  "TEN_TINH_THANH" = '';
        update "DWH"."DIM_NHA_CC" set  "TEN_TINH_THANH" = 'Bình Định' where  "TEN_TINH_THANH" = null;
        update "DWH"."DIM_NHA_CC" set  "TEN_TINH_THANH" = 'Gia Lai' where  "TEN_TINH_THANH" = 'HN';
        update "DWH"."DIM_NHA_CC" set  "TEN_TINH_THANH" =  'Đồng Nai' where "TEN_TINH_THANH" = 'HCM';
        
        """
        cursor.execute(update_sql_query)
        conn.commit()
        return {"table STG_VUNG_MIEN ": "Data imported successful"}
    # [END load_function]

    # [START check_data]
    def check_data_imported():
        hook = PostgresHook(postgres_conn_id="dwh_etl")
        df = hook.get_pandas_df(sql='SELECT * FROM "DWH"."DIM_NHA_CC"')
        return {"status ": "Success !"}
    # [END check_data]

    select_date_stg = PythonOperator(
        task_id='select_stg',
        python_callable=load,
    )
    select_date_stg.doc_md = dedent(
        """\
    #### Extract task
    Extract data from source brand table
    """
    )

    insert_data_dim = PythonOperator(
        task_id='insert_data',
        python_callable=load,
    )
    insert_data_dim.doc_md = dedent(
        """\
    #### Transform task
    Add identity column for data
    """
    )

    update_data_dim = PythonOperator(
        task_id='update_data',
        python_callable=load,
    )
    update_data_dim.doc_md = dedent(
        """\
    #### Load task
    Import data to dwh database
    """
    )
    select_dim = PythonOperator(
        task_id='select_dim',
        python_callable=check_data_imported
    )
    select_dim.doc_md=dedent(
        """
        #### Check data import
        Print total data imported !
        """
    )

    select_date_stg >> insert_data_dim >> update_data_dim >> select_dim

# [END main_flow]

# [END tutorial]