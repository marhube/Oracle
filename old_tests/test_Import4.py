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
    
    def test_replace_date_format(self):
        print('Er nå inne i TestImport.test_replace_date_format')     
        std_format = "YYYY-MM-DD"
        std_percent_format = imp.replace_time_format(std_format)
        print(f'std_oracle_format er {std_percent_format}')
        self.assertEqual(std_percent_format ,'%Y-%m-%d')   

    def test_sql_insert_into(self):
        print('Er nå inne i TestImport.test_sql_insert_into som ikke er ferdig implementert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        test_data['date_only_na'] = test_data['datetime_only_na']
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        sql_insert = SQL_Insert(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        str_insert_into = sql_insert.sql_insert_into()
        print("str_insert_into er")
        print(str_insert_into)
      
    # Tester kun at konvererter til datetime.date
    def test_preprocess_data(self):
        print('Er nå inne i TestImport.test_preprocess_data')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        test_data['date_only_na'] = test_data['datetime_only_na']
        test_data['date_datetime'] = test_data['datetime_with_na']
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        date_cols.append('date_datetime')
        sql_insert = SQL_Insert(test_data.dtypes,table_name=table_name,date_cols=date_cols)        
        data_to_insert = sql_insert.preprocess_data(df=test_data)
        date_col_indices = []
        #Memo to self: The last column is a bit special
        comparison_data = test_data.copy()
        comparison_data['date_datetime'] = comparison_data['date_datetime'].map(lambda x: x.date())
        #
        for enum,col in enumerate(comparison_data.columns):
            if col in date_cols:
                date_col_indices.append(enum)
        #
        subresults = [] 
        #Memo to self: The case where original data is
        for row,rowdata in enumerate(data_to_insert):           
            for ind in date_col_indices:           
                subresult = (
                    (rowdata[ind] is None and pd.isna(comparison_data.iloc[row,ind])) or 
                    (isinstance(rowdata[ind],str) and 
                     rowdata[ind] == str(comparison_data.iloc[row,ind])
                    )                     
                )               
                subresults.append(subresult) 
                if not subresult:                    
                    print(f'row er {row} og ind er {ind}')
                    print(f'rowdata[ind] er {rowdata[ind]} og test_data.iloc[row,ind] er {test_data.iloc[row,ind]}')
                    sys.exit()      
        #        
               
        self.assertTrue(pd.Series(subresults).all()) 
    
       
    
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

        
        
    

        
    