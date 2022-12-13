from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
from sqlalchemy import create_engine
from liendoan_ld.general_function.connect import Connection, MYDAG_DIM


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

        if self.db_type == "oracle":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            conn = OracleHook.get_connection(self.connection)
            engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            df1 = pd.read_json(data)
            df = df1.copy()
            print(df1)
            # convert date stg
            for col in date_col:
                df[col] = pd.to_datetime(df[col]).dt.date
            # print(df[col])
            df.to_sql('{}'.format(table_name), con=engine,
                      schema=schema_name, if_exists='append', index=False)
            print(df)
            return {"table {} ".format(table_name): "Data imported successful"}

    # ---------------------------------- Insert data for SUM -----------------------------------------------------------------------

    def insert_data_sum(self, ti, table_name: str, schema_name: str, date_col: list, task_ids: str, key: str, etl_date_var: str = ""):
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
            if schema_name == "DWH_SUM":
                for col in date_col:
                    df[col] = pd.to_datetime(df[col]).dt.date

            df.to_sql('{}'.format(table_name), con=engine,
                      schema=schema_name, if_exists='append', index=False)
            return {"table {} ".format(table_name): "Data imported successful"}

        if self.db_type == "oracle":
            data = ti.xcom_pull(task_ids=task_ids, key=key)
            conn = OracleHook.get_connection(self.connection)
            engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            df1 = pd.read_json(data)
            df = df1.copy()
            print(df1)

            if etl_date_var:
                df['NGAY_ETL'] = int(etl_date_var)
            else:
                ETL_DATE = pd.to_datetime("today").strftime("%Y%m%d")
                df['NGAY_ETL'] = int(ETL_DATE)

            # convert date stg
            if schema_name == "DWH_STG":
                for col in date_col:
                    df[col] = pd.to_datetime(df[col]).dt.date
            if schema_name == "DWH_SUM":
                for col in date_col:
                    print(df[col])
                    df[col] = pd.to_datetime(df[col], unit='ms').dt.date
                    print(df[col])
            #     #
            df.to_sql('{}'.format(table_name), con=engine,
                      schema=schema_name, if_exists='append', index=False)
            print(df)
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
            print(df)
            return {"status ": "Success !"}

    #       df['NAM'] = pd.to_datetime(df['NAM'], format='%d/%m/%y').dt.strftime('%Y-%m-%d')
    #       ti.xcom_push('kdl', df.to_json(orient='records'))

    def inner_join(self, index_key, idx2, idx_origin, idx_update, name_columns, new_record_df):
        df1 = pd.concat([new_record_df.iloc[:, idx2],
                        new_record_df.iloc[:, idx_origin]], axis=1)
        df1.columns = name_columns
        df2 = pd.concat([new_record_df.iloc[:, idx2],
                        new_record_df.iloc[:, idx_update]], axis=1)
        df2.columns = name_columns
        df3 = pd.concat([df1, df2]).drop_duplicates(
            name_columns, keep='last').sort_values(index_key)
        update_record_df = df3[df3.index.duplicated(keep='first') == True]
        return update_record_df

    def split_table(self, index_key, name_columns, df, other):
        change_columns = name_columns.copy()
        print("change_columns: ", change_columns)
        if not isinstance(index_key, list):
            change_columns.remove(index_key)
            index_key = [index_key]
        else:
            for x in index_key:
                change_columns.remove(x)
        # print(other.columns.to_list())
        temp_df = df.set_index(index_key).join(other.set_index(
            index_key), lsuffix='_origin', rsuffix='_update', how='outer').reset_index()
        # temp_df = pd.merge(df, other, on=index_key, suffixes=["_origin", "_update"], how='outer', indicator=True)
        print("name_columns split: ", name_columns)
        print(temp_df)
        temp_columns = temp_df.columns.to_list()

        idx1 = [temp_columns.index(x) for x in temp_columns if "_update" in x]
        idx2 = [name_columns.index(id) for id in index_key]

        print(temp_columns)
        new_record_df = temp_df[temp_df.isnull().any(axis=1)]
        deleted_record_df = new_record_df[(
            new_record_df.iloc[:, idx1].isnull().any(axis=1)).to_list()]
        new_record_df = new_record_df[(
            ~new_record_df.iloc[:, idx1].isnull().any(axis=1)).to_list()]
        print("deleted_record_df: ", deleted_record_df)

        deleted_record_df = deleted_record_df[index_key]
        print(deleted_record_df)

        # check new record ()
        if not new_record_df.empty:
            print("new_record_df: ", new_record_df)
            print(new_record_df.columns.to_list())

            new_record_df = pd.concat(
                [new_record_df.iloc[:, idx2], new_record_df.iloc[:, idx1]], axis=1)
            print("new_record_df: ", new_record_df)
            print(new_record_df.columns.to_list())
            new_record_df.columns = name_columns
        else:
            new_record_df = pd.DataFrame(columns=name_columns)
        # C2
        # update_record_df = temp_df[temp_df['_merge']=='both']

        # C1
        update_record_df = temp_df[~temp_df.isnull().any(axis=1)]

        update_column = update_record_df.columns.to_list()
        idx_update = [update_column.index(x)
                      for x in update_column if "_update" in x]
        idx_origin = [update_column.index(x)
                      for x in update_column if "_origin" in x]
        update_record_df = self.inner_join(
            index_key, idx2, idx_origin, idx_update, name_columns, update_record_df)
        return new_record_df, update_record_df, deleted_record_df

    def scd1_insert_database(self, new_record_df, engine, myDag, name_columns):
        for index, row in new_record_df.iterrows():
            sql = """ BEGIN 
                INSERT INTO "{}"."{}"
                ({})
                VALUES ({});
                END;
                """.format(myDag.SCHEMA_TARGET, myDag.TABLE_TARGET,
                           str(name_columns).replace(
                               "[", "").replace("]", "").replace("\'", "\"").upper(),
                           str(row.to_list()).replace("[", "").replace("]", ""))
            print(sql)
            try:
                with engine.begin() as conn:
                    conn.execute(sql)
                    print("success insert")
            except:
                raise Exception("Fail insert")

    def scd1_update_database(self, update_record_df, engine, myDag):
        for index, row in update_record_df.iterrows():
            sql = """ BEGIN 
                UPDATE "{}"."{}"
                SET "TEN_CHUC_VU" = '{}'
                WHERE "{}" = '{}';
                END;
                """.format(myDag.SCHEMA_TARGET, myDag.TABLE_TARGET, row.to_list()[-1],
                           str(myDag.TABLE_KEY).replace(
                               "[", "").replace("]", "").replace("\'", "\"").replace("\"", ""), row.to_list()[0])
            print(sql)
            try:
                with engine.begin() as conn:
                    conn.execute(sql)
                    print("success insert")
            except:
                raise Exception("Fail insert")

    def scd2_insert_database(self, new_record_df, engine, myDag, name_columns):
        for index, row in new_record_df.iterrows():
            sql = """ 
                BEGIN 
                INSERT INTO "{}"."{}"
                ({},"TRANG_THAI_BG","NGAY_HL_BG","NGAY_HH_BG")
                VALUES ({},'A', CURRENT_DATE, TO_DATE('2099-01-01','YYYY/MM/DD'));
                END;
                """.format(myDag.SCHEMA_TARGET, myDag.TABLE_TARGET,
                           str(name_columns).replace(
                               "[", "").replace("]", "").replace("\'", "\"").upper(),

                           str(row.to_list()).replace("[", "").replace("]", ""))
            print(sql)
            try:
                with engine.begin() as conn:
                    conn.execute(sql)
                    print("success insert")
            except:
                raise Exception("Fail insert")

    def scd2_update_database(self, update_record_df, engine, myDag):
        for index, row in update_record_df.iterrows():
            # sql = """ BEGIN
            #     UPDATE "{}"."{}"
            #     SET "TEN_CHUC_VU" = '{}'
            #     WHERE "{}" = '{}';
            #     END;
            #     """.format(myDag.SCHEMA_TARGET, myDag.TABLE_TARGET, row.to_list()[-1],
            #                myDag.TABLE_KEY, row.to_list()[0])
            print(row)
            print(row.to_list()[-1])
            # sql = """ BEGIN
            #     UPDATE "{}"."{}"
            #     SET "TEN_CHUC_VU" = '{}'
            #     WHERE "{}" = '{}';
            #     END;
            #     """.format(myDag.SCHEMA_TARGET, myDag.TABLE_TARGET, row.to_list()[-1],
            #                myDag.TABLE_KEY, row.to_list()[0])

            # print(sql)
            # try:
            #     with engine.begin() as conn:
            #         conn.execute(sql)
            #         print("success insert")
            # except:
            #     raise Exception("Fail insert")
