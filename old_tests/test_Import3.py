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
from Oracle.PrepData import robust_get_data_type_from_values


import unittest


class TestImport(unittest.TestCase):
    
    def test_replace_date_format(self):
        print('Er nå inne i TestImport.test_replace_date_format')     
        std_format = "YYYY-MM-DD"
        std_percent_format = imp.replace_date_format(std_format)
        print(f'std_oracle_format er {std_percent_format}')
        self.assertEqual(std_percent_format ,'%Y-%m-%d')   

    def test_sql_insert(self):
        print('Er nå inne i TestImport.test_sql_insert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = test_data = generate_test_data(test_n,include_all_missing = True)
        test_data['date_only_na'] = test_data['datetime_only_na']
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        sql_insert = SQL_Insert(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        print(f'sql_insert er {sql_insert}')
      
        
        
    
    
    # Tester kun at konvererter til datetime.date
    def test_prepare_data_to_insert(self):
        print('Er nå inne i TestImport.test_prepare_data_to_insert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = test_data = generate_test_data(test_n,include_all_missing = True)
        test_data['date_only_na'] = test_data['datetime_only_na']
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        sql_insert = SQL_Insert(test_data.dtypes,table_name=table_name,date_cols=date_cols)        
        prepared_data = sql_insert.prepare_data_to_insert(df=test_data)
        date_col_indices = []
        
        for enum,col in enumerate(test_data.columns):
            if col in date_cols:
                date_col_indices.append(enum)
        #
        subresults = [] 
        for row,prepdata in enumerate(prepared_data):
            for ind in date_col_indices:
                #subresult = isinstance(prepdata[ind],dt.date)
                #if test_data.iloc[row,ind] is None:
                #    subresult = prepdata[ind] is None
                #date_col = list(test_data.columns)[ind]
                #originally_none = test_date
                subresult = (
                    (prepdata[ind] is None and pd.isna(test_data.iloc[row,ind])) or 
                    isinstance(prepdata[ind],dt.date)
                    )               
                print(f'Er inne i date_col_indices der prepData[ind] er {prepdata[ind]} og subresult er {subresult}')
                print(f'test_data.iloc[row,ind] er {test_data.iloc[row,ind]}')
                subresults.append(subresult)
        #        
               
        self.assertTrue(pd.Series(subresults).all()) 

        
    