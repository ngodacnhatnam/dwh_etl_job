from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
from airflow.hooks.base import BaseHook
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR

from sqlalchemy import func

dbtype_hook={"postgres":PostgresHook,
             "oracle":OracleHook}
# connection input src
class Connection():
    db_type: str
    connection:str
    def __init__(self, db_type: str, connection):
        self.db_type =db_type
        self.connection = connection
        
    def postgre_conn(self,ti, query:str):
        conn = BaseHook.get_connection(self.connection)
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        df = pd.read_sql(query, con=engine)
        print(f"FOUND {len(df)}  records !")
        df.drop_duplicates()
        return df
        # ti.xcom_push('extract_data', df.to_json(orient='records'))
        
    def convert_date_to_date(self, df,column_name):

        df[column_name] = pd.to_datetime(df[column_name], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
        # ti.xcom_push('kdl', df.to_json(orient='records'))
        return df    
    
    def convert_number_to_date(self, df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%d/%m/%y').dt.strftime('%Y-%m-%d')
        # ti.xcom_push('kdl', df.to_json(orient='records'))
        return df
    
    def convert_string_to_date(self, df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%d/%m/%y')
        # ti.xcom_push('kdl', df.to_json(orient='records'))
        return df

class ProcessingData():
    db_type: str
    connection:str
    
    def __init__(self, db_type: str, connection):
        self.db_type =db_type
        self.connection = connection 
    
    def transform(self,ti,task_ids: str,key :str):
        if self.db_type=="postgres":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            df1 = pd.read_json(data)
            df = df1.copy()
            return df
            # ti.xcom_push('transform_data', df.to_json(orient='records'))            
        if self.db_type=="oracle":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            df1 = pd.read_json(data)
            df = df1.copy()
            return df
            # ti.xcom_push('transform_data', df.to_json(orient='records'))
    
    def truncate(self,table_name:str, schema_name:str):
        if self.db_type=="postgres":
            hook = PostgresHook(self.connection)
            conn = hook.get_conn()
            cursor = conn.cursor()
            delete_sql_query = """
            TRUNCATE TABLE "{}"."{}";
            """.format(schema_name,table_name)
            cursor.execute(delete_sql_query)
            conn.commit()           
        if self.db_type=="oracle":
            hook = OracleHook(self.connection)
            conn = hook.get_conn()            
            cursor = conn.cursor()
            delete_sql_query = """
            TRUNCATE TABLE "{}"."{}"
            """.format(schema_name,table_name)
            cursor.execute(delete_sql_query)
            conn.commit()   
        
    def insert_data(self,ti,table_name:str, schema_name:str,task_ids:str, key:str):
        if self.db_type=="postgres":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            conn = PostgresHook.get_connection(self.connection)
            engine = create_engine(
                f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            df1 = pd.read_json(data)
            df = df1.copy()
            print(df.dtypes)
            print(df)
            df.to_sql('{}'.format(table_name), con=engine,
                    schema='STG', if_exists='replace', index=False)
            return {"table {} ".format(table_name): "Data imported successful"} 
        if self.db_type=="oracle":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            conn = OracleHook.get_connection(self.connection)
            engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            df1 = pd.read_json(data)
            df = df1.copy()
            
            df["NGAY_SINH"] = pd.to_datetime(df["NGAY_SINH"]).dt.date
            df["NGAY_VAO_CD"] = pd.to_datetime(df["NGAY_VAO_CD"]).dt.date
            df["SO_CMND"] = df["SO_CMND"].astype(str)
            df["NAM_VAO_LAM_VIEC"] = df["NAM_VAO_LAM_VIEC"].astype(str)
            
            df.to_sql('{}'.format(table_name), con=engine,schema=schema_name, if_exists='append', index=False)
            
            return {"table {} ".format(table_name): "Data imported successful"}
    
    def check_data(self,table_name:str, schema_name:str):
        if self.db_type=="postgres":
            conn = PostgresHook.get_connection(self.connection)
            engine = create_engine(
                f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            query = 'select * from "{}"."{}"'.format(schema_name,table_name)
            df = pd.read_sql(query, con=engine)
            print(f"FOUND : {len(df)} record location imported !")
            return {"status ": "Success !"}
        if self.db_type=="oracle":
            conn = OracleHook.get_connection(self.connection)
            engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            query = 'select * from "{}"."{}"'.format(schema_name,table_name)
            df = pd.read_sql(query, con=engine)
            print(f"FOUND : {len(df)} record location imported !")
            return {"status ": "Success !"}
        
    #       df['NAM'] = pd.to_datetime(df['NAM'], format='%d/%m/%y').dt.strftime('%Y-%m-%d')
    #       ti.xcom_push('kdl', df.to_json(orient='records'))
    def convert_number_to_date(self, df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%d/%m/%y').dt.strftime('%Y-%m-%d')
        # ti.xcom_push('kdl', df.to_json(orient='records'))
        return df
            
    