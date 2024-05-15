import os
import sys
import re
#Forsøk på  systematisk testing
import pandas as pd


import unittest
import Oracle
from TestData import generate_test_data

#*********** Start Lager  testdata
test_n = 10
test_data = generate_test_data(test_n)
#*********** Slutt Lager testdata


class TestOracle(unittest.TestCase):
    def test_replace_env_variables(self):
        print('Er nå inne i test_replace_env_variables')        
        string_with_variables = "Path is: ${PATH}, and home is: ${HOME}"
        result = Oracle.replace_env_variables(string_with_variables)
        self.assertIsInstance(result,str) 
    
    def test_generate_select(self):
        print('Er nå inne i test_generate_select')  
        table_name = 'MY_TABLE'
        select_table = Oracle.generate_select(table_name)
        self.assertEqual(select_table,f'SELECT *  FROM {table_name}')

    def test_get_class_of_series(self):
        print('Er nå inne i test_get_class_of_series')
        subresults = [
            Oracle.get_class_of_series(test_data['float_with_na']) == 'float64',
            Oracle.get_class_of_series(test_data['float_no_na']) == 'float64',
            Oracle.get_class_of_series(test_data['int_with_na']) == 'int64',
            Oracle.get_class_of_series(test_data['int_no_na']) == 'int64',
            Oracle.get_class_of_series(test_data['datetime_with_na']) == 'datetime64',
            Oracle.get_class_of_series(test_data['datetime_no_na']) == 'datetime64',
            Oracle.get_class_of_series(test_data['date_with_na']) == 'object_',
            Oracle.get_class_of_series(test_data['date_no_na']) == 'object_',
            Oracle.get_class_of_series(test_data['str_with_na']) == 'object_',
            Oracle.get_class_of_series(test_data['str_no_na']) == 'object_'
        ]
        
        self.assertTrue(pd.Series(subresults).all()) 
    #        

    def test_extract_class_name(self):
        print('Er inne i test_extract_class_name')
        self.assertEqual(
            Oracle.extract_class_name("<class 'numpy.int64'>"),'numpy.int64'
            )


    def test_get_data_type_from_values(self):
        print('Er nå inne i test_get_data_type_from_values.')
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
            Oracle.get_data_type_from_values(test_data[col]) == test_data_type_dict[col] 
                for col in test_data.columns
        ]
        #

        self.assertTrue(pd.Series(subresults).all())   
        


if __name__ == '__main__':
    unittest.main()