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
    'etl_canbo_dim',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG DIM canbo',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl','dim'],
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
        df = hook.get_pandas_df(sql='''SELECT * FROM "STG"."STG_THONG_TIN_CAN_BO"''')
        
        insert_sql_query = '''INSERT INTO "DWH"."DIM_CAN_BO"("MA_CAN_BO","TEN_CAN_BO", "CHUC_VU", "PHONG_BAN", "TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
                            SELECT  TRIM(a."MA_CAN_BO"),   TRIM(a."TEN_CAN_BO"), TRIM(a."CHUC_VU"),  TRIM(a."PHONG_BAN"), 'A', to_date(a."NGAY_NHAN_CHUC",'dd/mm/YYYY'), '2099-01-01'::DATE
                            FROM "STG"."STG_THONG_TIN_CAN_BO" a
                            WHERE a."MA_CAN_BO" not in (select "MA_CAN_BO" from "DWH"."DIM_CAN_BO")
                                  and a."TEN_CAN_BO" is not null and trim(a."TEN_CAN_BO") != ''
                            ORDER BY a."MA_CAN_BO";'''
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(insert_sql_query)
        conn.commit()
        # xóa dữ liệu nếu stg không có khỏi bảng dim, update dữ liệu về null 
        # 
        update_sql_query = """
        UPDATE "DWH"."DIM_CAN_BO" A
        SET "TRANG_THAI_BG" = 'C', "NGAY_HH_BG" = to_date(B."NGAY_NHAN_CHUC",'dd/mm/YYYY')
        from "STG"."STG_THONG_TIN_CAN_BO" B
        where A."MA_CAN_BO"  = B."MA_CAN_BO"  and A."TRANG_THAI_BG" = 'A' and (
        A."TEN_CAN_BO" != TRIM(B."TEN_CAN_BO") OR A."CHUC_VU" != TRIM(B."CHUC_VU") OR
        A."PHONG_BAN" != TRIM(B."PHONG_BAN") OR A."NGAY_HL_BG" != to_date(B."NGAY_NHAN_CHUC",'dd/mm/YYYY') 
        );

        INSERT INTO "DWH"."DIM_CAN_BO"("MA_CAN_BO","TEN_CAN_BO", "CHUC_VU", "PHONG_BAN", "TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
        SELECT  TRIM(a."MA_CAN_BO"),   TRIM(a."TEN_CAN_BO"), TRIM(a."CHUC_VU"),  TRIM(a."PHONG_BAN"), 'A', to_date(a."NGAY_NHAN_CHUC",'dd/mm/YYYY'), '2099-01-01'::DATE
        FROM "STG"."STG_THONG_TIN_CAN_BO" a
        WHERE a."MA_CAN_BO" not in (select "MA_CAN_BO" from "DWH"."DIM_CAN_BO" where "TRANG_THAI_BG" = 'A')
        ORDER BY a."MA_CAN_BO";
        """
        cursor.execute(update_sql_query)
        conn.commit()
        return {"table STG_THONG_TIN_CAN_BO ": "Data imported successful"}
    # [END load_function]

    # [START check_data]
    def check_data_imported():
        hook = PostgresHook(postgres_conn_id="dwh_etl")
        df = hook.get_pandas_df(sql='SELECT * FROM "DWH"."DIM_CAN_BO"')
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