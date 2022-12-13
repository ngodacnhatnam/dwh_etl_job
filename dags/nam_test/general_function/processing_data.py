from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
import datetime
import re
import string

class ProcessingData():
    db_type: str
    connection: str

    def __init__(self, db_type: str, connection):
        self.db_type = db_type
        self.connection = connection

    def transform(self, ti, task_ids: str, key: str):
        if self.db_type == "postgres":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            df1 = pd.read_json(data)
            df = df1.copy()
            return df

        if self.db_type == "oracle":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            print("data", data)
            df1 = pd.read_json(data)
            df = df1.copy()
            return df

    def delete_by_etl_date(self, table_name: str, schema_name: str, etl_col: str, etl_date_var: str):
        if self.db_type == "oracle":
            hook = OracleHook(self.connection)
            conn = hook.get_conn()
            cursor = conn.cursor()
            if etl_date_var == "":
                delete_sql_query = """
                DELETE FROM "{}"."{}"
                WHERE {} = TO_NUMBER(to_char(CURRENT_DATE ,'YYYYMMDD'))
                """.format(schema_name, table_name, etl_col)
                print(delete_sql_query)
                cursor.execute(delete_sql_query)
            else:
                delete_sql_query = """
                DELETE FROM "{}"."{}"
                WHERE {} = {}
                """.format(schema_name, table_name, etl_col, etl_date_var)
                print(delete_sql_query)
                cursor.execute(delete_sql_query)
            conn.commit()

    def truncate(self, table_name: str, schema_name: str):
        if self.db_type == "postgres":
            hook = PostgresHook(self.connection)
            conn = hook.get_conn()
            cursor = conn.cursor()
            delete_sql_query = """
                TRUNCATE TABLE "{}"."{}";
                """.format(schema_name, table_name)
            cursor.execute(delete_sql_query)
            conn.commit()
        if self.db_type == "oracle":
            hook = OracleHook(self.connection)
            conn = hook.get_conn()
            cursor = conn.cursor()
            delete_sql_query = """
                TRUNCATE TABLE "{}"."{}"
                """.format(schema_name, table_name)
            cursor.execute(delete_sql_query)
            conn.commit()

    # ---------------------------------- Insert data for STG -----------------------------------------------------------------------

    def insert_data_stg(self, ti, table_name: str, schema_name: str, date_col: list, task_ids: str, key: str):
        if self.db_type == "postgres":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            conn = PostgresHook.get_connection(self.connection)
            engine = create_engine(
                f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            df1 = pd.read_json(data)
            df = df1.copy()

            # process date_col here
            for col in date_col:
                df[col] = pd.to_datetime(df[col]).dt.date
            df.to_sql('{}'.format(table_name), con=engine,
                      schema=schema_name, if_exists='replace', index=False)
            return {"table {} ".format(table_name): "Data imported successful"}

        # ------------------------------------------Oracle insert ----------------------------------------------------------------
        if self.db_type == "oracle":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            conn = OracleHook.get_connection(self.connection)
            engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            df1 = pd.read_json(data)
            df = df1.copy()

            # convert date stg
            if schema_name == "DWH_STG":
                for col in date_col:
                    df[col] = pd.to_datetime(df[col]).dt.date

            df.to_sql('{}'.format(table_name), con=engine,
                      schema=schema_name, if_exists='append', index=False)

            return {"table {} ".format(table_name): "Data imported successful"}


    def check_data(self, table_name: str, schema_name: str):
        if self.db_type == "postgres":
            conn = PostgresHook.get_connection(self.connection)
            engine = create_engine(
                f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            query = 'select * from "{}"."{}"'.format(schema_name, table_name)
            df = pd.read_sql(query, con=engine)
            print(f"FOUND : {len(df)} record location imported !")
            return {"status ": "Success !"}
        if self.db_type == "oracle":
            conn = OracleHook.get_connection(self.connection)
            engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            query = 'select * from "{}"."{}"'.format(schema_name, table_name)
            df = pd.read_sql(query, con=engine)

            print(f"FOUND : {len(df)} record location imported !")
            return {"status ": "Success !"}

    #       df['NAM'] = pd.to_datetime(df['NAM'], format='%d/%m/%y').dt.strftime('%Y-%m-%d')
    #       ti.xcom_push('kdl', df.to_json(orient='records'))
    def convert_number_to_date(self, df, column_name):
        df[column_name] = pd.to_datetime(
            df[column_name], format='%d/%m/%y').dt.strftime('%Y-%m-%d')
        # ti.xcom_push('kdl', df.to_json(orient='records'))
        return df

    def convert_df_stg_to_target(self, specific_cols: dict, df: pd.DataFrame):
        print(specific_cols,
              '****************************************************************')
        df1 = pd.DataFrame()
        print(df.columns.names)
        for key, val in specific_cols.items():
            print(df[key])
            # df1[val]= df[key]
        return df1
