import pandas as pd


def convert_string_to_number(self, df, column_name):
    df[column_name] = df[column_name].str.replace(r'[^\w\s]+', '')
    return df
