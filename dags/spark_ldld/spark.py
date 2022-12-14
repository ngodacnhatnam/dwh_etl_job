import os
import pyspark
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from textwrap import dedent
from airflow.providers.oracle.hooks.oracle import OracleHook
import pyspark.sql.functions as f
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-class-path ojdbc7-12.1.0.2.jar --jars ojdbc7-12.1.0.2.jar --master local[3] pyspark-shell"

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

conn = OracleHook.get_connection('oracle_dwh')
engine = create_engine(
    f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

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

