from airflow.hooks.base import BaseHook
import json
from textwrap import dedent
import pendulum
import pandas as pd
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from sqlalchemy import create_engine
# Operators; we need this to operate!
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
# from adhoc.connect import Connection, ProcessingData
from liendoan_ld.general_function.connect import Connection, MYDAG_DIM
from liendoan_ld.general_function.processing_data import ProcessingData

mypath = '/home/airflow/airflow/dags/liendoan_ld/declare/dim/dim_danh_sach_lien_doan.json'
myDag = MYDAG_DIM(mypath)
# [END import_module]

# [START instantiate_dag]
with DAG(
    myDag.AIRFLOW_NAME,
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 0},
    description=myDag.DESCRIPTION,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=myDag.TAGS
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    connection_src = Connection(myDag.TABLE_SOURCE, myDag.CONNECTION_SOURCE)
    processing_target = ProcessingData(
        myDag.DB_TYPE_TARGET, myDag.CONNECTION_TARGET)

    conn = OracleHook.get_connection('dwh_oracle')
    engine = create_engine(
        f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    # [START extract_function]

    def extract(**kwargs):
        # all col stg, dim
        str_cols = ','.join(myDag.ALL_COL)
        query_stg = """select * from "{}"."{}" """.format(
            myDag.SCHEMA_SOURCE, myDag.TABLE_SOURCE)
        df_stg = connection_src.oracle_conn(query=query_stg)
        # print(df_stg)
        query_dim = """select {} from "{}"."{}" """.format(str_cols,
                                                           myDag.SCHEMA_TARGET, myDag.TABLE_TARGET)
        df_dim = connection_src.oracle_conn(query=query_dim)

        name_columns = ["MA_LIEN_DOAN", "TEN_LIEN_DOAN",
                        "DIA_CHI", "SO_DT", "TEN_LD_CAP_TREN"]
        index_key = name_columns[0]

        new_df, update_df, _ = processing_target.split_table(
            index_key, name_columns, df_dim, df_stg)

        if df_dim.empty:
            REUSE_COL = ["ma_lien_doan", "ten_lien_doan", "dia_chi"]
            new_df = df_stg.loc[:, REUSE_COL]
            name_columns = df_dim.columns.to_list()[1:4]
            print("name_columns", name_columns)
            processing_target.scd2_insert_database(
                new_df, engine, myDag, name_columns)
        else:
            if not new_df.empty:
                REUSE_COL = ["ma_lien_doan", "ten_lien_doan", "dia_chi"]
                new_df = df_stg.loc[:, REUSE_COL]
                name_columns = df_dim.columns.to_list()[1:4]
                print("name_columns", name_columns)
                processing_target.scd2_update_database(new_df, engine, myDag)

    def check_data_imported():
        processing_target.check_data(myDag.TABLE_TARGET, myDag.SCHEMA_TARGET)
    # [END load_function]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """ Extract task
            Extract data from source catalog table, unique value
        """
    )

    check_import_task = PythonOperator(
        task_id='check_import',
        python_callable=check_data_imported
    )
    check_import_task.doc_md = dedent(
        """ Check data import
            Print total data imported !
        """
    )
    extract_task >> check_import_task
