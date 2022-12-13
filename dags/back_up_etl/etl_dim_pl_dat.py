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
    'etl_dim_pl_dat',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG DIM phan loai dat',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl','dim', 'daklak'],
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
        df = hook.get_pandas_df(sql='''SELECT * FROM "STG"."STG_PHAN_LOAI_DAT"''')
        
        insert_sql_query = '''INSERT INTO "DWH"."DIM_PHAN_LOAI_DAT"("MA_LOAI_DAT_CAP1" , "TEN_LOAI_DAT_CAP1" , "MA_LOAI_DAT_CAP2" , "TEN_LOAI_DAT_CAP2" , "MA_LOAI_DAT_CAP3" ,"TEN_LOAI_DAT_CAP3" ,"MA_MAP_LOAI_DAT" ,"TRANG_THAI_BG", "NGAY_HL_BG","NGAY_HH_BG")
                                SELECT "MA_LOAI_DAT_CAP1" , "TEN_LOAI_DAT_CAP1" ,
                                        case when "MA_LOAI_DAT_CAP2" is null then "MA_LOAI_DAT_CAP1" 
                                            else "MA_LOAI_DAT_CAP2" END,
                                        case when "MA_LOAI_DAT_CAP2" is null then "TEN_LOAI_DAT_CAP1" 
                                            else "TEN_LOAI_DAT_CAP1" end,
                                        case when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is null then "MA_LOAI_DAT_CAP1" 
                                            when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is not null then "MA_LOAI_DAT_CAP2" 
                                            else "MA_LOAI_DAT_CAP3" end as mldc3,
                                        case when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is null then "TEN_LOAI_DAT_CAP1"  
                                            when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is not null then "TEN_LOAI_DAT_CAP2"  
                                            else "TEN_LOAI_DAT_CAP3" end,
                                            "MA_MAP_LOAI_DAT"  ,'A', current_date , '2099-01-01'::DATE
                                FROM "STG"."STG_PHAN_LOAI_DAT" a
                                WHERE a."MA_LOAI_DAT_CAP3" not in (select "MA_LOAI_DAT_CAP3" from "DWH"."DIM_PHAN_LOAI_DAT")
                                    and a."MA_LOAI_DAT_CAP2" not in (select "MA_LOAI_DAT_CAP3" from "DWH"."DIM_PHAN_LOAI_DAT")
                                    and a."MA_LOAI_DAT_CAP1" not in (select "MA_LOAI_DAT_CAP3" from "DWH"."DIM_PHAN_LOAI_DAT")
                                ORDER BY mldc3;'''
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(insert_sql_query)
        conn.commit()
        # xóa dữ liệu nếu stg không có khỏi bảng dim, update dữ liệu về null 
        # 
        update_sql_query = """
        WITH tmp as (
        SELECT "MA_LOAI_DAT_CAP1" , "TEN_LOAI_DAT_CAP1" ,
                case when "MA_LOAI_DAT_CAP2" is null then "MA_LOAI_DAT_CAP1" 
                    else "MA_LOAI_DAT_CAP2" END as "MA_LOAI_DAT_CAP2",
                case when "MA_LOAI_DAT_CAP2" is null then "TEN_LOAI_DAT_CAP1" 
                    else "TEN_LOAI_DAT_CAP2" end as "TEN_LOAI_DAT_CAP2",
                case when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is null then "MA_LOAI_DAT_CAP1" 
                    when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is not null then "MA_LOAI_DAT_CAP2" 
                    else "MA_LOAI_DAT_CAP3" end as mldc3,
                case when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is null then "TEN_LOAI_DAT_CAP1"  
                    when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is not null then "TEN_LOAI_DAT_CAP2"  
                    else "TEN_LOAI_DAT_CAP3" end as "TEN_LOAI_DAT_CAP3",
                    "MA_MAP_LOAI_DAT"
        FROM "STG"."STG_PHAN_LOAI_DAT" a
        ORDER BY mldc3
        )
        UPDATE "DWH"."DIM_PHAN_LOAI_DAT" A
        SET "TRANG_THAI_BG" = 'C', "NGAY_HH_BG" = current_date 
        FROM tmp B
        WHERE A."MA_LOAI_DAT_CAP3"  = B."mldc3"  AND A."TRANG_THAI_BG" = 'A' AND (
        A."MA_LOAI_DAT_CAP1" != TRIM(B."MA_LOAI_DAT_CAP1") OR A."TEN_LOAI_DAT_CAP1" != TRIM(B."TEN_LOAI_DAT_CAP1") OR A."MA_LOAI_DAT_CAP2" != TRIM(B."MA_LOAI_DAT_CAP2")
        OR A."TEN_LOAI_DAT_CAP2" != TRIM(B."TEN_LOAI_DAT_CAP2") OR A."TEN_LOAI_DAT_CAP3" != TRIM(B."TEN_LOAI_DAT_CAP3") OR A."MA_MAP_LOAI_DAT" != TRIM(B."MA_MAP_LOAI_DAT") 
        );

        WITH tmp as (
        SELECT "MA_LOAI_DAT_CAP1" , "TEN_LOAI_DAT_CAP1" ,
                case when "MA_LOAI_DAT_CAP2" is null then "MA_LOAI_DAT_CAP1" 
                    else "MA_LOAI_DAT_CAP2" END as "MA_LOAI_DAT_CAP2",
                case when "MA_LOAI_DAT_CAP2" is null then "TEN_LOAI_DAT_CAP1" 
                    else "TEN_LOAI_DAT_CAP2" end as "TEN_LOAI_DAT_CAP2",
                case when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is null then "MA_LOAI_DAT_CAP1" 
                    when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is not null then "MA_LOAI_DAT_CAP2" 
                    else "MA_LOAI_DAT_CAP3" end as mldc3,
                case when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is null then "TEN_LOAI_DAT_CAP1"  
                    when "MA_LOAI_DAT_CAP3" is null and "MA_LOAI_DAT_CAP2" is not null then "TEN_LOAI_DAT_CAP2"  
                    else "TEN_LOAI_DAT_CAP3" end as "TEN_LOAI_DAT_CAP3",
                    "MA_MAP_LOAI_DAT"
        FROM "STG"."STG_PHAN_LOAI_DAT" a
        ORDER BY mldc3
        )
        INSERT INTO "DWH"."DIM_PHAN_LOAI_DAT"("MA_LOAI_DAT_CAP1" , "TEN_LOAI_DAT_CAP1" , "MA_LOAI_DAT_CAP2" , "TEN_LOAI_DAT_CAP2" , "MA_LOAI_DAT_CAP3" ,"TEN_LOAI_DAT_CAP3" ,"MA_MAP_LOAI_DAT" ,"TRANG_THAI_BG", "NGAY_HL_BG","NGAY_HH_BG")
        SELECT "MA_LOAI_DAT_CAP1" , "TEN_LOAI_DAT_CAP1" ,
                case when "MA_LOAI_DAT_CAP2" is null then "MA_LOAI_DAT_CAP1" 
                    else "MA_LOAI_DAT_CAP2" END as MA_LOAI_DAT_CAP2,
                case when "MA_LOAI_DAT_CAP2" is null then "TEN_LOAI_DAT_CAP1" 
                    else "TEN_LOAI_DAT_CAP2" end as TEN_LOAI_DAT_CAP2 ,
                case when mldc3 is null and "MA_LOAI_DAT_CAP2" is null then "MA_LOAI_DAT_CAP1" 
                    when mldc3 is null and "MA_LOAI_DAT_CAP2" is not null then "MA_LOAI_DAT_CAP2" 
                    else mldc3 end as mldc3,
                case when mldc3 is null and "MA_LOAI_DAT_CAP2" is null then "TEN_LOAI_DAT_CAP1"  
                    when mldc3 is null and "MA_LOAI_DAT_CAP2" is not null then "TEN_LOAI_DAT_CAP2"  
                    else "TEN_LOAI_DAT_CAP3" end as TEN_LOAI_DAT_CAP3,
                    "MA_MAP_LOAI_DAT",'A', current_date , '2099-01-01'::DATE
        FROM tmp a
        WHERE a.mldc3 not in (select "MA_LOAI_DAT_CAP3" from "DWH"."DIM_PHAN_LOAI_DAT" where "TRANG_THAI_BG" = 'A')
        ORDER BY mldc3;

        """
        cursor.execute(update_sql_query)
        conn.commit()
        return {"table DIM_PHAN_LOAI_DAT ": "Data imported successful"}
    # [END load_function]

    # [START check_data]
    def check_data_imported():
        hook = PostgresHook(postgres_conn_id="daklak_dwh")
        df = hook.get_pandas_df(sql='SELECT * FROM "DWH"."DIM_PHAN_LOAI_DAT"')
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