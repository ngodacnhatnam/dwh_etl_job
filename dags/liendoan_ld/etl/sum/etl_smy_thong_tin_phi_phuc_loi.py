from airflow.hooks.base import BaseHook
import json
from textwrap import dedent
import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
# from adhoc.connect import Connection, ProcessingData
from liendoan_ld.general_function.connect import Connection, MYDAG_SUM
from liendoan_ld.general_function.processing_data import ProcessingData

mypath = '/home/airflow/airflow/dags/liendoan_ld/declare/sum/smy_thong_tin_phi_phuc_loi.json'
myDag = MYDAG_SUM(mypath)
# [END import_module]

# [START instantiate_dag]
with DAG(
    myDag.AIRFLOW_NAME,
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 0},
    description=myDag.DESCRIPTION,
    schedule_interval=None,
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

    # [START extract_function]
    def extract(**kwargs):
        target_cols = myDag.ALL_COL
        ti = kwargs['ti']
        str_cols = ','.join(myDag.ALL_COL)
        print("******", myDag.TABLE_SOURCE, "*** ", myDag.SCHEMA_SOURCE)
        query = """select {} from "{}"."{}" """.format(str_cols,
            myDag.SCHEMA_SOURCE, myDag.TABLE_SOURCE)
        df = connection_src.oracle_conn(ti, query=query)
        ti.xcom_push('extract_data', df.to_json(orient='records'))
    # [END extract_function]

   # [START transform_function]
    def transform(ti):
        print("START Tranforms")
        df = processing_target.transform(
            ti, task_ids='extract', key='extract_data')
        ti.xcom_push('transform_data', df.to_json(orient='records'))

    # [END transform_function]

     # [START load_function]
    def load(ti):
        print("START LOAD TO DES DB")
        etl_date_var = myDag.ETL_DATE_VAR
        processing_target.delete_by_etl_date(
            table_name=myDag.TABLE_TARGET, schema_name=myDag.SCHEMA_TARGET,etl_col=myDag.ETL_DATE_COL,etl_date_var=etl_date_var)
        processing_target.insert_data_sum(
            ti, table_name=myDag.TABLE_TARGET, schema_name=myDag.SCHEMA_TARGET, date_col=myDag.DATE_COL, 
            task_ids="transform", key="transform_data",etl_date_var=etl_date_var)
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

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """ Transform task
            Add identity column for data
        """
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """ Load task
            Import data to dwh database
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
    extract_task >> transform_task >> load_task >> check_import_task

# [END main_flow]
