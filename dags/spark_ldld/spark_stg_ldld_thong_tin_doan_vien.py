import os
import pyspark
from pyspark.sql import SparkSession
from textwrap import dedent
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-class-path ojdbc7-12.1.0.2.jar --jars ojdbc7-12.1.0.2.jar --master local[3] pyspark-shell"
with DAG(
    'spark_stg_thong_tin_doan_vien',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 0},
    description='ETL DAG Import',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl', 'spark'],
) as dag:
    dag.doc_md = __doc__

    appName = "PySpark Example - Oracle Example"
    master = "local"

    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .config('spark.jars', 'ojdbc7-12.1.0.2.jar') \
        .getOrCreate()

    sql_select = """ SELECT * FROM "DWH_STG"."STG_LDLD_THONG_TIN_DOAN_VIEN" """
    sql_truncate = """ TRUNCATE TABLE "DWH_STG"."STG_LDLD_THONG_TIN_DOAN_VIEN1" """
    user = "ODI_REPO"
    password = "Vdc2022"
    server = "172.16.50.74"
    port = 1521
    service_name = 'oracle19db'
    jdbcUrl = f"jdbc:oracle:thin:@{server}:{port}:{service_name}"
    jdbcDriver = "oracle.jdbc.driver.OracleDriver"

    def insert(**kwargs):
        jdbcDF = spark.read.format("jdbc") \
            .option("url", jdbcUrl) \
            .option("query", sql_select) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", jdbcDriver) \
            .load()
        jdbcDF.write.format('jdbc').options(
            url=f"jdbc:oracle:thin:@{server}:{port}:{service_name}",
            driver='oracle.jdbc.driver.OracleDriver',
            dbtable='DWH_STG.STG_LDLD_THONG_TIN_DOAN_VIEN1',
            user='ODI_REPO',
            password='Vdc2022').mode('overwrite').save()
    print(1)
    insert_task = PythonOperator(
        task_id='insert_task',
        python_callable=insert,
    )
    insert_task.doc_md = dedent(
        """ Insert task
            Import data to dwh database
        """
    )

    insert_task
