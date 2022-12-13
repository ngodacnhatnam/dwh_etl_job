import string
import re
import pandas as pd

def convert_string_to_number(df, column_name):
        df[column_name] = df[column_name].str.replace('[{}]'.format(string.punctuation), '')
        df[column_name] = df[column_name].str.replace(" ", "")
        df[column_name] = df[column_name].str.replace(r'\D', '')
        df[column_name] = pd.to_numeric(df[column_name])
        return df
