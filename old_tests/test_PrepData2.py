import os
import sys
#**********
import pandas as pd
import numpy as np
import datetime as dt
#********

#Forsøk på  systematisk testing
from TestData import generate_test_data
import Oracle.PrepData as PrepData
from Oracle.PrepData import PreprocessData



import unittest



class TestPrepData(unittest.TestCase):
    def test_get_class_of_series(self):
        print('Er nå inne i TestPrepData.test_get_class_of_series')
        test_data = generate_test_data(10)
        
        subresults = [
            PrepData.get_class_of_series(test_data['float_with_na']) == 'float64',
            PrepData.get_class_of_series(test_data['float_no_na']) == 'float64',
            PrepData.get_class_of_series(test_data['int_with_na']) == 'int64',
            PrepData.get_class_of_series(test_data['int_no_na']) == 'int64',
            PrepData.get_class_of_series(test_data['datetime_with_na']) == 'datetime64',
            PrepData.get_class_of_series(test_data['datetime_no_na']) == 'datetime64',
            PrepData.get_class_of_series(test_data['date_with_na']) == 'object_',
            PrepData.get_class_of_series(test_data['date_no_na']) == 'object_',
            PrepData.get_class_of_series(test_data['str_with_na']) == 'object_',
            PrepData.get_class_of_series(test_data['str_no_na']) == 'object_'
        ]
        
        self.assertTrue(pd.Series(subresults).all()) 
    #           
    
    def test_extract_class_name(self):
        print('Er inne i TestPrepDataImport.test_extract_class_name')
        self.assertEqual(
            PrepData.extract_class_name("<class 'numpy.int64'>"),'numpy.int64'
            )

    def test_get_data_type_from_values(self):
        print('Er nå inne i TestPrepData.test_get_data_type_from_values.')
        
        test_data = generate_test_data(10)
        
        test_data_type_dict = {}
        test_data_type_dict['float_with_na'] = 'float'
        test_data_type_dict['float_no_na'] = 'float'
        test_data_type_dict['int_with_na'] = 'numpy.int64'
        test_data_type_dict['int_no_na'] = 'numpy.int64'
        test_data_type_dict['datetime_with_na'] = 'pandas._libs.tslibs.timestamps.Timestamp'
        test_data_type_dict['datetime_no_na'] = 'pandas._libs.tslibs.timestamps.Timestamp'
        test_data_type_dict['date_with_na'] =  'datetime.date'
        test_data_type_dict['date_no_na'] = 'datetime.date'
        test_data_type_dict['str_with_na'] =  'str'
        test_data_type_dict['str_no_na'] = 'str'
        
        subresults = [
            PrepData.get_data_type_from_values(test_data[col]) == test_data_type_dict[col] 
                for col in test_data.columns
        ]
        #
        print(f'subresults er {subresults}')
        self.assertTrue(pd.Series(subresults).all())  
        
    
    def test_get_class_from_dtypes(self):
        print('Er nå inne i TestPrepData.test_get_class_from_dtypes')
        
        test_data = generate_test_data(10)
        
        class_dict = PrepData.get_classes_from_dtypes(test_data.dtypes)
        subresults = [
            class_dict['float_with_na'] == 'numpy.float64',
            class_dict['float_no_na'] == 'numpy.float64',
            class_dict['int_with_na'] == 'numpy.int64',
            class_dict['int_no_na'] == 'numpy.int64',
            class_dict['datetime_with_na'] == 'numpy.datetime64',
            class_dict['datetime_no_na'] == 'numpy.datetime64',
            class_dict['date_with_na'] == 'numpy.object_',
            class_dict['date_no_na'] == 'numpy.object_',
            class_dict['str_with_na'] == 'numpy.object_',
            class_dict['str_no_na'] == 'numpy.object_'            
            ]               
        #
        
        self.assertTrue(pd.Series(subresults).all())      
        
    
    def test_replace_date_format(self):
        print('Er nå inne i TestPrepData.test_replace_date_format')     
        std_format = "YYYY-MM-DD"
        std_percent_format = PrepData.replace_time_format(std_format)
        print(f'std_oracle_format er {std_percent_format}')
        self.assertEqual(std_percent_format ,'%Y-%m-%d')   

    def test_sql_insert_into(self):
        print('Er nå inne i TestPrepData.test_sql_insert_into som ikke er ferdig implementert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        test_data['date_only_na'] = test_data['datetime_only_na']
        data_types = PrepData.robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        str_insert_into = preprocess.sql_insert_into()
        print("str_insert_into er")
        print(str_insert_into)
      
    # Tester kun at konvererter til datetime.date
    def test_preprocess_data(self):
        print('Er nå inne i TestPrepData.test_preprocess_data')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        test_data['date_only_na'] = test_data['datetime_only_na']
        test_data['date_datetime'] = test_data['datetime_with_na']
        data_types = PrepData.robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        date_cols.append('date_datetime')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)        
        data_to_insert = preprocess.preprocess_data(df=test_data)
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