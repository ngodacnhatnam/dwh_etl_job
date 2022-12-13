from pendulum import datetime
from sqlalchemy import create_engine
from textwrap import dedent
import pendulum, pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from nam.general_function.connect import Connection, MYDAG
from nam.general_function.processing_data import ProcessingData
from nam.general_function.convert import convert_string_to_number

mypath = '/home/airflow/airflow/dags/nam/declare/stg/stg_doanh_nghiep_lu_hanh_dl.json'
myDag = MYDAG(mypath)

# [START instantiate_dag]
with DAG(
    myDag.AIRFLOW_NAME,
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 1},
    description=myDag.DESCRIPTION,
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=myDag.TAGS,
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
        ti = kwargs['ti']
        query1 = """select * from "{}"."{}"; """.format(
            myDag.SCHEMA_SOURCE, myDag.TABLE_SOURCE)
        query2 = """select * from "{}"."{}"; """.format(
            myDag.SCHEMA_SOURCE, myDag.TABLE_SOURCE_2)
        query3 = """select * from "{}"."{}"; """.format(
            myDag.SCHEMA_SOURCE, myDag.TABLE_SOURCE_3)
        query4 = """select * from "{}"."{}"; """.format(
            myDag.SCHEMA_SOURCE, myDag.TABLE_SOURCE_4)
        df1 = connection_src.postgre_conn(ti, query=query1)
        df2 = connection_src.postgre_conn(ti, query=query2)
        df3 = connection_src.postgre_conn(ti, query=query3)
        df4 = connection_src.postgre_conn(ti, query=query4)
        df5 = pd.merge(right=df2, left=df1,how='left')
        df6 = pd.merge(right=df3, left=df5, right_on='ten_trang_thai', left_on='trang_thai_dnlh', how='left')
        df = pd.merge(right=df4, left=df6, right_on='ten_loai_hinh', left_on='loai_doanh_nghiep_lh',how='left')
        df = df.drop(['ma_huyen_tp', 'ma_trang_thai_dnlh', 'ten_trang_thai', 'ma_loai_hinh_dnlh', 'ten_loai_hinh'], axis=1)
        df = convert_string_to_number(df, 'so_dien_thoai')
        print(df.head(5))
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
        processing_target.truncate(
            table_name=myDag.TABLE_TARGET, schema_name=myDag.SCHEMA_TARGET)
        processing_target.insert_data_stg(
            ti, table_name=myDag.TABLE_TARGET, schema_name=myDag.SCHEMA_TARGET, date_col=myDag.DATE_COL, task_ids="transform", key="transform_data")

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
