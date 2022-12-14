import os
import pyspark
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from textwrap import dedent
from airflow.providers.oracle.hooks.oracle import OracleHook
import pyspark.sql.functions as f
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-class-path ojdbc7-12.1.0.2.jar --jars ojdbc7-12.1.0.2.jar --master local[3] pyspark-shell"
with DAG(
    'spark_dim_chuc_vu_lien_doan',
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

    sql_select_stg = """ SELECT * FROM "DWH_STG"."STG_LDLD_CHUC_VU_LIEN_DOAN" """
    sql_select_dim = """ SELECT MA_CHUC_VU,TEN_CHUC_VU FROM "DWH_DIM"."DIM_LDLD_CHUC_VU_LIEN_DOAN1" """
    sql_select_delete_dim = """ SELECT * FROM "DWH_DIM"."DIM_LDLD_CHUC_VU_LIEN_DOAN1" """
    user = "ODI_REPO"
    password = "Vdc2022"
    server = "172.16.50.74"
    port = 1521
    service_name = 'oracle19db'
    jdbcUrl = f"jdbc:oracle:thin:@{server}:{port}:{service_name}"
    jdbcDriver = "oracle.jdbc.driver.OracleDriver"

    conn = OracleHook.get_connection('dwh_oracle')
    engine = create_engine(
        f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    def insert(**kwargs):
        def read_df(query):
            return spark.read.format("jdbc") \
            .option("url", jdbcUrl) \
            .option("query", query) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", jdbcDriver) \
            .load()
            

        df_stg = read_df(sql_select_stg)
            
        df_dim = spark.read.format("jdbc") \
            .option("url", jdbcUrl) \
            .option("query", sql_select_dim) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", jdbcDriver) \
            .load()

        df_readfull_dim = spark.read.format("jdbc") \
            .option("url", jdbcUrl) \
            .option("query", sql_select_delete_dim) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", jdbcDriver) \
            .load()

        # df_stg.show()

        if df_dim.rdd.isEmpty():
            df_stg.write.format('jdbc').options(
                url=f"jdbc:oracle:thin:@{server}:{port}:{service_name}",
                driver='oracle.jdbc.driver.OracleDriver',
                dbtable='DWH_DIM.DIM_LDLD_CHUC_VU_LIEN_DOAN1',
                user='ODI_REPO',
                password='Vdc2022').mode('append').save()
        else:
            tempdf = df_stg.subtract(df_dim)
            # tempdf.show()
            # print(tempdf.rdd.collect()[0])
            list_filter = tempdf.rdd.map(lambda x: x['MA_CHUC_VU']).collect()
            list_value = tempdf.rdd.map(lambda x: x['TEN_CHUC_VU']).collect()
            # df1 = df_readfull_dim.filter(df_readfull_dim.MA_CHUC_VU.isin(list_filter))['MA_CHUC_VU', 'TEN_CHUC_VU']
            # df1.show()

            sql = """ BEGIN 
                        UPDATE "DWH_DIM"."DIM_LDLD_CHUC_VU_LIEN_DOAN1"
                        SET "TEN_CHUC_VU" = '%s'
                        WHERE "MA_CHUC_VU" = '%s';
                        END;
                        """%(list_filter[0], list_value[0])
            print(sql)
            try:
                with engine.begin() as conn:
                    conn.execute(sql)
                    print("success insert")
            except:
                raise Exception("Fail insert")

                
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
