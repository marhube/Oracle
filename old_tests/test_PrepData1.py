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
import unittest



class TestPrepData(unittest.TestCase):
    def test_get_class_of_series(self):
        print('Er nå inne i TestPrepDataImport.test_get_class_of_series')
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
        print('Er nå inne i TestPrepDataImport.test_get_data_type_from_values.')
        
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
        print('Er nå inne i TestPrepDataImport.test_get_class_from_dtypes')
        
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
        
    
    def test_none_for_nan(self):     
        print('Er nå inne i TestPrepDataImport.test_none_for_nan')      
      
        datetimes = pd.DataFrame({'datetimes': [dt.datetime.now().date(),np.NaN]})
        new_datetimes = datetimes.copy()
        for col in new_datetimes.columns:
            new_datetimes[col] = PrepData.none_for_nan(new_datetimes[col])
        
        subresults = [
            new_datetimes.iloc[0,0] == dt.datetime.now().date(),
            new_datetimes.iloc[1,0] is None
        ]
        self.assertTrue(pd.Series(subresults).all())
        

    def test_date2datetime(self):
        print('Er nå inne i TestPrepDataImport.test_date2datetime')
        
        test_data = generate_test_data(10)        
        function_output = PrepData.date2datetime(test_data.copy())
        subresults = []
        for col in ['date_with_na','date_no_na']:
            for elem in function_output[col].to_list():
                subresults.append((pd.isna(elem)) or (isinstance(elem,dt.datetime)))
        #
        print('subresults er ')
        print(subresults)
                    
        self.assertTrue(pd.Series(subresults).all())   
    
    