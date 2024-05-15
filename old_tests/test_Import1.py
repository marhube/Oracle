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
        
    def test_instantiateSQL_Insert(self):
        print('Er nå inne i TestImport.test_instantiateSQL_Insert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        sql_insert = SQL_Insert(test_data.dtypes,table_name=table_name)
        self.assertIsInstance(sql_insert,SQL_Insert)   


    