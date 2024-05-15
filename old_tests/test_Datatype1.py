import os
import sys
#**********
import pandas as pd
import numpy as np
import datetime as dt
#********

#Forsøk på  systematisk testing
from TestData import generate_test_data
import Oracle.Datatype as Datatype

import unittest



class TestDatatype(unittest.TestCase):
    def test_get_class_of_series(self):
        print('Er nå inne i TestDatatype.test_get_class_of_series')
        test_data = generate_test_data(10)
        
        subresults = [
            Datatype.get_class_of_series(test_data['float_with_na']) == 'float64',
            Datatype.get_class_of_series(test_data['float_no_na']) == 'float64',
            Datatype.get_class_of_series(test_data['int_with_na']) == 'int64',
            Datatype.get_class_of_series(test_data['int_no_na']) == 'int64',
            Datatype.get_class_of_series(test_data['datetime_with_na']) == 'datetime64',
            Datatype.get_class_of_series(test_data['datetime_no_na']) == 'datetime64',
            Datatype.get_class_of_series(test_data['date_with_na']) == 'object_',
            Datatype.get_class_of_series(test_data['date_no_na']) == 'object_',
            Datatype.get_class_of_series(test_data['str_with_na']) == 'object_',
            Datatype.get_class_of_series(test_data['str_no_na']) == 'object_'
        ]
        
        self.assertTrue(pd.Series(subresults).all()) 
    #           
    
    def test_extract_class_name(self):
        print('Er inne i TestPrepDataImport.test_extract_class_name')
        self.assertEqual(
            Datatype.extract_class_name("<class 'numpy.int64'>"),'numpy.int64'
            )

    def test_get_data_type_from_values(self):
        print('Er nå inne i TestDatatype.test_get_data_type_from_values.')
        
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
            Datatype.get_data_type_from_values(test_data[col]) == test_data_type_dict[col] 
                for col in test_data.columns
        ]
        #
        print(f'subresults er {subresults}')
        self.assertTrue(pd.Series(subresults).all())  
        
    
    def test_get_class_from_dtypes(self):
        print('Er nå inne i TestDatatype.test_get_class_from_dtypes')
        
        test_data = generate_test_data(10)
        
        class_dict = Datatype.get_classes_from_dtypes(test_data.dtypes)
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
        
    
    