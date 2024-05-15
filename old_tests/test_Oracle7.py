import os
import sys
import re
#Forsøk på  systematisk testing
import unittest
import oracledb
import sqlalchemy
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from Oracle import Oracle 
from Oracle.Oracle import Oracle_Connect

#
#*********** Start Lager  testdata
# Function to generate random dates
datetime_cols = ['col3','col4']

def random_dates(start, end, precision, n=5):
    start_time = int(datetime.timestamp(start))
    end_time = int(datetime.timestamp(end))

    if precision == 'second':
        return [datetime.fromtimestamp(random.randint(start_time, end_time)).replace(microsecond=0) for _ in range(n)]
    elif precision == 'microsecond':
        return [datetime.fromtimestamp(random.uniform(start_time, end_time)) for _ in range(n)]

# Generate data for columns
np.random.seed(0)  # Seed for reproducibility

# Float values
col0 = np.random.rand(5)
col0[0] = np.nan  # Missing value

# String values
col1 = [f'str_{random.randint(0, 100)}' for _ in range(5)]
col1[1] = None  # Missing value

# Integer values using Nullable Integer Type
col2 = pd.array(np.random.randint(0, 100, 5), dtype=pd.Int64Dtype())
col2[2] = pd.NA  # Missing value

# Datetime values with second and microsecond precision
start_date = datetime(2020, 1, 1)
end_date = datetime(2021, 1, 1)
col3 = random_dates(start_date, end_date, 'second')
col3[3] = pd.NaT  # Missing value
col4 = random_dates(start_date, end_date, 'microsecond')
col4[4] = pd.NaT  # Missing value
col5 = [x.date() for x in col3]
col6 = [x.date() for x in col4]

# Create the DataFrame 
df = pd.DataFrame({
    'col0': col0,
    'col1': col1,
    'col2': col2,
    'col3': col3,
    'col4': col4,
    'col5': col5,
    'col6': col6
})

#*********** Slutt Lager testdata
print(f'df er {df}')

class TestOracle(unittest.TestCase):
    def test_replace_env_variables(self):
        string_with_variables = "Path is: ${PATH}, and home is: ${HOME}"
        result = Oracle.replace_env_variables(string_with_variables)
        self.assertIsInstance(result,str) 
    
    def test_generate_select(self):
        table_name = 'MY_TABLE'
        select_table = Oracle.generate_select(table_name)
        self.assertEqual(select_table,f'SELECT *  FROM {table_name}')
        
        
    def test_get_class_of_series(self):
        print('Er nå inne i test_get_class_of_series')
        self.assertTrue(
            (Oracle.get_class_of_series(df['col0']) == 'float64')  &
            (Oracle.get_class_of_series(df['col1']) == 'object_')  &
            (Oracle.get_class_of_series(df['col2']) == 'int64')  &
            (Oracle.get_class_of_series(df['col3']) == 'datetime64')  &
            (Oracle.get_class_of_series(df['col4']) == 'datetime64')  &
            (Oracle.get_class_of_series(df['col5']) == 'object_')  &
            (Oracle.get_class_of_series(df['col6']) == 'object_') 
            )            
    
    def test_extract_class_name(self):
        self.assertEqual(
            Oracle.extract_class_name("<class 'numpy.int64'>"),'numpy.int64'
            )
    
    def test_get_class_from_dtypes(self):
        class_dict = Oracle.get_classes_from_dtypes(df.dtypes)
        self.assertTrue(
            (class_dict['col0'] == 'numpy.float64')  &
            (class_dict['col1'] == 'numpy.object_')  &
            (class_dict['col2'] == 'numpy.int64')  &
            (class_dict['col3'] == 'numpy.datetime64')  &
            (class_dict['col4'] == 'numpy.datetime64')  &
            (class_dict['col5'] == 'numpy.object_')  &
            (class_dict['col6'] == 'numpy.object_')
            )
        
        
           
    # Må lage test som sjekker at 'col3' har like verdier som 'col5' og at
    #'col4' har like verdier oms 'col6' i de tilfeller der det ikke er null
    def test_datetime2date(self):
        print('Er nå inne i test_datetime2date')
        test_output = Oracle.datetime2date(df.copy())
        test_output['is_valid'] = test_output.apply(
            lambda x : (isinstance(x.col3,pd._libs.tslibs.nattype.NaTType)) &
                        (isinstance(x.col5,pd._libs.tslibs.nattype.NaTType)),
                        axis=1)
        #
        print(f'test_output er {test_output}')
        
       
        
        
        #print(test_output)
        #self.assertTrue(
        #    (isinstance(test_output['col3'], 
        #     == test_output['col5']).all() &
        #    (test_output['col4'] == test_output['col6']).all()
        #)

    
    def test_get_connection(self):
        conn = Oracle_Connect().get_connection()
        self.assertIsInstance(conn,oracledb.Connection)
        conn.close()
    
    #     
       
    
    def test_get_engine(self):
        oracle_connect=Oracle_Connect()
        engine = oracle_connect.get_engine()
        self.assertIsInstance(engine,sqlalchemy.engine.base.Engine)    
        engine.dispose()
        
    
        
      
if __name__ == '__main__':
    unittest.main()
