from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
import json

class MYDAG():

    def __init__(self,my_path):
        self.my_path = open(my_path, 'r')
        self.VARIABLE = json.load(self.my_path)
        self.TABLE_SOURCE = self.VARIABLE["TABLE_SOURCE"]
        self.SCHEMA_SOURCE = self.VARIABLE["SCHEMA_SOURCE"]
        self.DB_TYPE_SOURCE = self.VARIABLE["DB_TYPE_SOURCE"]
        self.CONNECTION_SOURCE = self.VARIABLE["CONNECTION_SOURCE"]
        self.TABLE_TARGET = self.VARIABLE["TABLE_TARGET"]
        self.SCHEMA_TARGET = self.VARIABLE["SCHEMA_TARGET"]
        self.DB_TYPE_TARGET = self.VARIABLE["DB_TYPE_TARGET"]
        self.CONNECTION_TARGET = self.VARIABLE["CONNECTION_TARGET"]
        self.ALL_COL = self.VARIABLE["ALL_COL"]
        self.DATE_COL = self.VARIABLE["DATE_COL"]
        self.DAG_CONFIG = self.VARIABLE["DAG_CONFIG"]
        self.AIRFLOW_NAME = self.DAG_CONFIG["AIRFLOW_NAME"]
        self.DESCRIPTION = self.DAG_CONFIG["DESCRIPTION"]
        self.TAGS = self.DAG_CONFIG["TAGS"]

class MYDAG_SUM():

    def __init__(self,my_path):
        self.my_path = open(my_path, 'r')
        self.VARIABLE = json.load(self.my_path)
        self.TABLE_SOURCE = self.VARIABLE["TABLE_SOURCE"]
        self.SCHEMA_SOURCE = self.VARIABLE["SCHEMA_SOURCE"]
        self.DB_TYPE_SOURCE = self.VARIABLE["DB_TYPE_SOURCE"]
        self.CONNECTION_SOURCE = self.VARIABLE["CONNECTION_SOURCE"]
        self.TABLE_TARGET = self.VARIABLE["TABLE_TARGET"]
        self.SCHEMA_TARGET = self.VARIABLE["SCHEMA_TARGET"]
        self.DB_TYPE_TARGET = self.VARIABLE["DB_TYPE_TARGET"]
        self.CONNECTION_TARGET = self.VARIABLE["CONNECTION_TARGET"]
        self.ALL_COL = self.VARIABLE["ALL_COL"]
        self.DATE_COL = self.VARIABLE["DATE_COL"]
        self.ETL_DATE_COL = self.VARIABLE["ETL_DATE_COL"]
        self.ETL_DATE_VAR = self.VARIABLE["ETL_DATE_VAR"]
        self.DAG_CONFIG = self.VARIABLE["DAG_CONFIG"]
        self.AIRFLOW_NAME = self.DAG_CONFIG["AIRFLOW_NAME"]
        self.DESCRIPTION = self.DAG_CONFIG["DESCRIPTION"]
        self.TAGS = self.DAG_CONFIG["TAGS"]
class Connection():
    db_type: str
    connection: str

    def __init__(self, db_type: str, connection):
        self.db_type = db_type
        self.connection = connection

    def postgre_conn(self, ti, query: str):
        conn = BaseHook.get_connection(self.connection)
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        df = pd.read_sql(query, con=engine)
        print(f"FOUND {len(df)}  records !")
        df.drop_duplicates()
        return df
        # ti.xcom_push('extract_data', df.to_json(orient='records'))

    def oracle_conn(self,ti,query:str):
        conn = OracleHook.get_connection(self.connection)
        engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        df = pd.read_sql(query, con=engine)
        print(f"FOUND {len(df)}  records !")
        df.drop_duplicates()
        return df
    
        
    def convert_date_to_date(self, df, column_name):
        df[column_name] = pd.to_datetime(
            df[column_name], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
        # ti.xcom_push('kdl', df.to_json(orient='records'))
        return df

    def convert_string_to_date(self, df, column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%d/%m/%y')
        # ti.xcom_push('kdl', df.to_json(orient='records'))
        return df

    def execute_oracle_query(self,ti,query:str):
        conn = OracleHook.get_connection(self.connection)
        engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        df = pd.read_sql(query, con=engine)
        print(f"FOUND {len(df)}  records !")
        df.drop_duplicates()
        return df
    
    # def process_query():
        
        
        
        
        