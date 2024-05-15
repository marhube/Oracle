import os
import sys
#**********
import pandas as pd
import numpy as np
import datetime as dt
#********

#Forsøk på  systematisk testing
from TestData import generate_test_data
import Oracle.Import as imp
from Oracle.Import import SQL_Insert
from Oracle.SQL import SQL_Create
from Oracle.PrepData import robust_get_data_type_from_values
from Oracle.Oracle_Connect import Connect
from Oracle.Execute import Exec,drop_user_table_if_exists

import unittest


class TestImport(unittest.TestCase):
    

    
    def test_create_table(self):
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        sql_insert = SQL_Insert(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        conn = Connect().get_connection()
        drop_user_table_if_exists(table_name = table_name, conn = conn)
        str_create_table = sql_insert.create_user_table(conn)
        print(f'str_create_table er {str_create_table}')
       
    
    def test_create_or_replace_table(self):
        print('Er nå inne i TestImport.test_create_or_replace_persistent_table som ikke er ferdig implementert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        sql_insert = SQL_Insert(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        str_insert_into = sql_insert.sql_insert_into() 
        data_to_insert = sql_insert.preprocess_data(df=test_data)
        conn = Connect().get_connection()
        drop_user_table_if_exists(table_name = table_name, conn = conn)
        sql_create = SQL_Create(sql_insert)
        str_create_table = sql_create.sql_create_table()
        crsr = conn.cursor()
        crsr.execute(str_create_table)
        conn.commit()
        crsr.executemany(str_insert_into,data_to_insert)
        conn.commit()
        crsr.close()
        conn.close()

        
        
    

        
    