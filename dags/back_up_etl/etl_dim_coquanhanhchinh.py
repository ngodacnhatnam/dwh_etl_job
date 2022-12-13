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
    'etl_coquanhanhchinh_dim',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG DIM khoang san',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl', 'dim', 'daklak'],
) as dag:

    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    # # [START load_function]

    def load(**kwargs):
        print("START LOAD TO DES DB")
        # thêm mới dữ liệu từ bảng staging in dim
        hook = PostgresHook(postgres_conn_id="daklak_dwh")
        df = hook.get_pandas_df(
            sql='''SELECT * FROM "STG"."STG_DON_VI_HANH_CHINH"''')

        insert_sql_query = '''INSERT INTO "DWH"."DIM_DON_VI_HANH_CHINH"("MA_DON_VI","TEN_DON_VI", "TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
                            SELECT  TRIM(a."MA_DON_VI"),   TRIM(a."TEN_DON_VI"), 'A', current_date, '2099-01-01'::DATE
                            FROM "STG"."STG_DON_VI_HANH_CHINH" a
                            WHERE a."MA_DON_VI" not in (select "MA_DON_VI" from "DWH"."DIM_DON_VI_HANH_CHINH")
                                  and a."TEN_DON_VI" is not null and trim(a."TEN_DON_VI") != ''
                            ORDER BY a."TEN_DON_VI";'''
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(insert_sql_query)
        conn.commit()
        # xóa dữ liệu nếu stg không có khỏi bảng dim, update dữ liệu về null
        #
        update_sql_query = """
        UPDATE "DWH"."DIM_DON_VI_HANH_CHINH" A
        SET "TRANG_THAI_BG" = 'C', "NGAY_HH_BG" = current_date
        from "STG"."STG_DON_VI_HANH_CHINH" B
        where A."MA_DON_VI"  = B."MA_DON_VI"  and A."TRANG_THAI_BG" = 'A' and 
        A."TEN_DON_VI" != TRIM(B."TEN_DON_VI") 
        ;

        INSERT INTO "DWH"."DIM_DON_VI_HANH_CHINH"("MA_DON_VI","TEN_DON_VI", "TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
        SELECT  TRIM(a."MA_DON_VI"),   TRIM(a."TEN_DON_VI"), 'A', current_date, '2099-01-01'::DATE
        FROM "STG"."STG_DON_VI_HANH_CHINH" a
        WHERE a."MA_DON_VI" not in (select "MA_DON_VI" from "DWH"."DIM_DON_VI_HANH_CHINH" where "TRANG_THAI_BG" = 'A')
        ORDER BY a."MA_DON_VI";
        """
        cursor.execute(update_sql_query)
        conn.commit()
        return {"table STG_DON_VI_HANH_CHINH ": "Data imported successful"}
    # [END load_function]

    # [START check_data]
    def check_data_imported():
        hook = PostgresHook(postgres_conn_id="daklak_dwh")
        df = hook.get_pandas_df(
            sql='SELECT * FROM "DWH"."DIM_DON_VI_HANH_CHINH"')
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
    select_dim.doc_md = dedent(
        """
        #### Check data import
        Print total data imported !
        """
    )

    select_date_stg >> insert_data_dim >> update_data_dim >> select_dim

# [END main_flow]

# [END tutorial]
