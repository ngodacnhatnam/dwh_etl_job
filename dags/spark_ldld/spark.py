import os
import pyspark
from pyspark.sql import SparkSession
from textwrap import dedent


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
user = "ODI_REPO"
password = "Vdc2022"
server = "172.16.50.74"
port = 1521
service_name = 'oracle19db'
jdbcUrl = f"jdbc:oracle:thin:@{server}:{port}:{service_name}"
jdbcDriver = "oracle.jdbc.driver.OracleDriver"


df_stg = spark.read.format("jdbc") \
    .option("url", jdbcUrl) \
    .option("query", sql_select_stg) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", jdbcDriver) \
    .load()
    
df_dim = spark.read.format("jdbc") \
    .option("url", jdbcUrl) \
    .option("query", sql_select_dim) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", jdbcDriver) \
    .load()

df_stg.show()
# print(df_dim.rdd.isEmpty())

if df_dim.rdd.isEmpty():
    df_stg.write.format('jdbc').options(
        url=f"jdbc:oracle:thin:@{server}:{port}:{service_name}",
        driver='oracle.jdbc.driver.OracleDriver',
        dbtable='DWH_DIM.DIM_LDLD_CHUC_VU_LIEN_DOAN1',
        user='ODI_REPO',
        password='Vdc2022').mode('overwrite').save()
else:
    tempdf = df_stg.subtract(df_dim)
    tempdf.show()
    # sql_querry = """ DELETE FROM DWH_DIM.DIM_LDLD_CHUC_VU_LIEN_DOAN1 """
    # df_delete = spark.read.format("jdbc") \
    # .option("url", jdbcUrl) \
    # .option("query", sql_querry) \
    # .option("user", user) \
    # .option("password", password) \
    # .option("driver", jdbcDriver) \
    # .load()
    # list_delete = tempdf.select("MA_CHUC_VU").rdd.map(lambda x : x[0]).collect()
    # for i in list_delete:
    # sql_querry = """ DELETE FROM DWH_DIM.DIM_LDLD_CHUC_VU_LIEN_DOAN1  WHERE MA_CHUC_VU = 'CV002' """

    # sql_querry = """ SELECT MA_CHUC_VU,TEN_CHUC_VU FROM DIM_LDLD_CHUC_VU_LIEN_DOAN1 """
    # print(sql_querry)
    # spark.sql(sql_querry).show()
    
    # df_delete.write.format('jdbc').options(
    #     url=f"jdbc:oracle:thin:@{server}:{port}:{service_name}",
    #     driver='oracle.jdbc.driver.OracleDriver',
    #     dbtable='DWH_DIM.DIM_LDLD_CHUC_VU_LIEN_DOAN1',
    #     user='ODI_REPO',
    #     password='Vdc2022').mode('overwrite').save()