from adhoc.connect import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import pandas as pd
import logging
from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime
from sqlalchemy.types import (VARCHAR, DATE, INT)


class SCDType2 ():
    source_columns = pd.DataFrame
    target_columns = pd.DataFrame

    def __init__(self, target_dict: dict, source_dict: dict, key_column: str, map_column: dict, columns_changed: list[str] = []):
        """
        Args:
            - map_column(dict):  {
                "column_name_1": "column_name_2",
                "column_name_2": "column_name_3"
            }
            - source_dict/target_dict(dict): {
                "db_type": "postgres",
                "connection": "dwh_postgres",
                "table_name": "table_name",
                "schema_name": "schema_name",
            }
        """
        self.target_dict = target_dict
        self.source_dict = source_dict
        self.key_column = key_column
        self.map_column = map_column
        self.source_columns = self.get_columns_prop(
            db_dict=source_dict, is_source=True)
        self.target_columns = self.get_columns_prop(db_dict=target_dict)
        self.columns_changed = columns_changed

    def conn_engine(self, is_source: bool = False):
        table_name = self.source_dict['table_name'] if is_source else self.target_dict['table_name']
        schema_name = self.source_dict['schema_name'] if is_source else self.target_dict['schema_name']
        connection = self.source_dict['connection'] if is_source else self.target_dict['connection']
        db_type = self.source_dict['db_type'] if is_source else self.target_dict['db_type']
        conn = PostgresHook.get_connection(connection)
        engine = create_engine(
            f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        if db_type == "oracle":
            conn = OracleHook.get_connection(connection)
            engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        return conn, engine, table_name, schema_name, connection, db_type

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

    def get_columns_prop(self, db_dict: dict, is_source: bool = False):
        """Return dataframe of property table destination
        Exam: [
            {"column_name":"COLUMN_NAME","data_type":"NUMBER" }
        ]
        Args: 
            - db_dict(dict): Database options dict
        """
        sql = ""
        conn, engine, table_name, schema_name, connection, db_type = self.conn_engine(
            is_source=is_source)
        if db_dict['db_type'] == 'oracle':
            sql = "select column_name, data_type from ALL_TAB_COLUMNS where TABLE_NAME='{}'".format(
                table_name)
            df_columns = pd.read_sql(sql, engine)
            print("df_columns: ", df_columns)
            return df_columns

    def split_table(self, index_key, name_columns, df, other):
        change_columns = name_columns.copy()
        print("change_columns: ", change_columns)
        if not isinstance(index_key, list):
            change_columns.remove(index_key)
            index_key = [index_key]
        else:
            for x in index_key:
                change_columns.remove(x)
        temp_df = df.set_index(index_key).join(other.set_index(
            index_key), lsuffix='_origin', rsuffix='_update', how='outer').reset_index()
        # temp_df = pd.merge(df, other, on=index_key, suffixes=["_origin", "_update"], how='outer', indicator=True)
        temp_columns = temp_df.columns.to_list()

        idx1 = [temp_columns.index(x) for x in temp_columns if "_update" in x]
        idx2 = [name_columns.index(id) for id in index_key]

        new_record_df = temp_df[temp_df.isnull().any(axis=1)]
        deleted_record_df = new_record_df[(
            new_record_df.iloc[:, idx1].isnull().any(axis=1)).to_list()]
        new_record_df = new_record_df[(
            ~new_record_df.iloc[:, idx1].isnull().any(axis=1)).to_list()]
        deleted_record_df = deleted_record_df[index_key]
        # check new record ()
        if not new_record_df.empty:
            new_record_df = pd.concat(
                [new_record_df.iloc[:, idx2], new_record_df.iloc[:, idx1]], axis=1)
            new_record_df.columns = name_columns
        else:
            new_record_df = pd.DataFrame(columns=name_columns)
        update_record_df = temp_df[~temp_df.isnull().any(axis=1)]
        update_column = update_record_df.columns.to_list()
        idx_update = [update_column.index(x)
                      for x in update_column if "_update" in x]
        idx_origin = [update_column.index(x)
                      for x in update_column if "_origin" in x]
        update_record_df = self.inner_join(
            index_key, idx2, idx_origin, idx_update, name_columns, update_record_df)
        return new_record_df, update_record_df, deleted_record_df

    def process(self):
        df_source = self.get_df_source()
        df_target = self.get_df_target()
        print("df_source : ")
        print(df_source)
        print("df_target : ")
        # df_target.columns = [x.upper() for x in df_target.columns]
        print(df_target)
        df_insert = df_source[~df_source[self.key_column].isin(
            df_target[self.key_column])]
        print("df_insert:", df_insert)
        self.insert(df_insert)

        df_delete = df_target[~df_target[self.key_column].isin(
            df_source[self.key_column])]
        print("df_delete:", df_delete)

        name_columns = df_source.columns.to_list()
        index_key = name_columns[0]
        new_record_df, update_record_df, deleted_record_df = self.split_table(
            index_key, name_columns, df_target, df_source)
        print("new_record_df: ", new_record_df)
        print("update_record_df: ", update_record_df)
        print("deleted_record_df: ", deleted_record_df)

        # self.insert(df_delete)

    def delete(self):
        conn, engine, table_name, schema_name, connection, db_type = self.conn_engine()
        sql = """
        
        """

    def update(self):
        """ update old records on dim if data has deleted, need to add new key value
            Set :
                - NGAY_HH_BG = Current Date
                - TRANG_THAI_BG= 'C'
            AND
            Insert data changed
        """

        """Update records if data has changed
            Set :
                - NGAY_HH_BG = Current Date
                - TRANG_THAI_BG= 'C'
            AND
            Insert data changed
        """
        # insert data changed into temp table
        conn, engine, table_name, schema_name, connection, db_type = self.conn_engine()
        # insert temp table
        df_changed = self.get_df_changed()
        print("df_changed: ", df_changed)
        if df_changed.empty:
            return None
        # print("df_changed dtypes : ", df_changed_normalized.dtypes)
        dtype = {}
        for i, v in df_changed.dtypes.items():
            if "int" in str(v):
                dtype[i] = INT
            elif "date" in str(v) or "NGAY_" in i:
                dtype[i] = DATE
            else:
                dtype[i] = VARCHAR(1000)
        for idx, val in self.target_columns.iterrows():
            if val['column_name'] in df_changed.columns:
                if val['data_type'] == 'DATE':
                    df_changed[val['column_name']] = df_changed[val['column_name']].map(
                        lambda x: datetime.strptime(str(x), '%Y-%m-%d'))
        print("dtype : ", dtype)
        df_changed.to_sql('temp_{}'.format(table_name), con=engine,
                          schema=schema_name, dtype=dtype, if_exists='replace', index=False)
        # update DIM table
        sql = """
            UPDATE "{schema_name}"."{table_name}" t1
            SET t1."NGAY_HH_BG"=CURRENT_DATE, t1."TRANG_THAI_BG"='C' 
            WHERE EXISTS (
                SELECT "{key_column}" FROM  "{schema_name}"."temp_{table_name}" t2 WHERE t1."{key_column}" =t2."{key_column}"
            )

        """.format(
            schema_name=schema_name,
            table_name=table_name,
            key_column=self.key_column,
        )
        engine.execute(sql)
        print(sql)
        print("Updated success !")
        # Insert data
        cols = []
        for idx, val in self.target_columns.iterrows():
            cols.append(val['column_name'])
        cols.pop(0)
        cols.remove("NGAY_HH_BG")
        cols.remove("NGAY_HL_BG")
        cols.remove("TRANG_THAI_BG")
        columns_tar = ','.join(cols)
        sql_insert = """
        INSERT INTO "{schema_name}"."{table_name}" ({cols},NGAY_HH_BG,TRANG_THAI_BG,NGAY_HL_BG)
        SELECT {cols}, TO_DATE('2099-01-01','YYYY-MM-DD') AS NGAY_HH_BG, 'A' AS TRANG_THAI_BG, CURRENT_DATE AS NGAY_HL_BG
        FROM "{schema_name}"."temp_{table_name}" a
        """.format(
            schema_name=schema_name,
            table_name=table_name,
            cols=columns_tar
        )
        print("SQL : ", sql_insert)
        engine.execute(sql_insert)
        print("Insert success !")

    def get_df_source(self):
        conn, engine, table_name, schema_name, connection, db_type = self.conn_engine(
            is_source=True)
        columns = ""
        for i, item in enumerate(self.map_column):
            columns += "{} AS {}".format(item, self.map_column[item])
            if i < len(self.map_column)-1:
                columns += " , "
        sql = """
            SELECT {} FROM "{}"."{}"
        """.format(columns, schema_name, table_name)
        print("SQL get_df_source : ", sql)
        df = pd.read_sql(sql, engine)
        return self.normalized_df(df)

    def get_df_target(self):
        conn, engine, table_name, schema_name, connection, db_type = self.conn_engine()
        columns = ""
        for i, item in enumerate(self.map_column):
            columns += "{}".format(self.map_column[item])
            if i < len(self.map_column)-1:
                columns += " , "
        sql = """
            SELECT {} FROM "{}"."{}" WHERE "TRANG_THAI_BG"='A'
        """.format(columns, schema_name, table_name)
        print("SQL get_df_target : ", sql)
        df = pd.read_sql(sql, engine)
        return self.normalized_df(df)

    def get_df_changed(self):
        """Return dataframe changed  from source table and target table      
        """
        df_source = self.get_df_source()
        df_target = self.get_df_target()
        on_columns = [*self.columns_changed, *[self.key_column]]
        # df_merged = df_source.merge(
        #     df_target, on=on_columns, how='left', indicator=True)
        # df_changed = df_merged[df_merged['_merge'] == 'left_only']
        name_columns = df_source.columns.to_list()
        index_key = name_columns[0]
        new_record_df, update_record_df, deleted_record_df = self.split_table(
            index_key, name_columns, df_target, df_source)
        df_changed = update_record_df

        if df_changed.empty:
            print("*************** NO CHANGED ! *************")
            return pd.DataFrame()
        df_changed_onsource = df_source[df_source[self.key_column].isin(
            df_changed[self.key_column])]
        print("df_changed_onsource : ", df_changed_onsource)
        return self.normalized_df(df=df_changed_onsource)

    def insert(self, df: pd.DataFrame):
        """Insert dataframe into the database

        Keyword arguments:
        df -- dataframe to be inserted        
        """
        conn, engine, table_name, schema_name, connection, db_type = self.conn_engine()
        df['TRANG_THAI_BG'] = 'A'
        df['NGAY_HL_BG'] = datetime.today().strftime('%Y-%m-%d')
        df['NGAY_HH_BG'] = datetime(2099, 1, 1).strftime('%Y-%m-%d')
        df = df.rename(columns=str.upper)
        dtype = {}
        print(" df.dtypes ", df.dtypes)
        # for i, v in df.dtypes.items():
        #     if "int" in str(v):
        #         dtype[i]= INT
        #     elif "datetime" in str(v) or "NGAY_" in df.dtypes[i]:
        #         dtype[i]=DATE
        #     else:
        #         dtype[i]=VARCHAR(1000)
        for idx, val in self.target_columns.iterrows():
            if val['data_type'] == 'DATE':
                if val['column_name'] in df.columns:
                    df[val['column_name']] = df[val['column_name']].map(
                        lambda x: datetime.strptime(str(x), '%Y-%m-%d'))
        print("***** dtype: ", dtype)
        if db_type == "oracle":
            conn = OracleHook.get_connection(connection)
            engine = create_engine(
                f'oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        try:
            df.to_sql('{}'.format(table_name), con=engine,
                      schema=schema_name, if_exists='append', index=False)
        except Exception as e:
            logging.error(e)

    # xu ly clob, datetime
    def normalized_df(self, df: pd.DataFrame, debug: bool = False):
        dtypes = df.dtypes
        df1 = df.copy()
        for i, v in dtypes.items():
            if "object" in str(v):
                df1[i] = df1[i].map(lambda x: str(x))
                if debug:
                    print("**************** DEBUG : ", i, " ***** ", v)
            if "datetime" in str(v):
                df1[i] = df1[i].map(lambda x: x.strftime('%Y-%m-%d'))
        return df1.rename(columns=str.upper)
