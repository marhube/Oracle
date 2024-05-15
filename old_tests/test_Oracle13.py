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
from datetime import datetime, date
from Oracle import Oracle 
from Oracle.Oracle import Oracle_Connect
from TestData import generate_test_data

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


# Create the DataFrame 
df = pd.DataFrame({
    'col0': col0,
    'col1': col1,
    'col2': col2,
    'col3': col3,
    'col4': col4
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
            (Oracle.get_class_of_series(df['col4']) == 'datetime64')  
            )            
    
    def test_extract_class_name(self):
        self.assertEqual(
            Oracle.extract_class_name("<class 'numpy.int64'>"),'numpy.int64'
            )
    
    def test_get_class_from_dtypes(self):
        print('Er nå inne i test_get_class_from_dtypes')
        class_dict = Oracle.get_classes_from_dtypes(df.dtypes)
        subresults = [
            class_dict['col0'] == 'numpy.float64',
            class_dict['col1'] == 'numpy.object_',
            class_dict['col2'] == 'numpy.int64',
            class_dict['col3'] == 'numpy.datetime64',
            class_dict['col4'] == 'numpy.datetime64'
            ]               
        #
        self.assertTrue(pd.Series(subresults).all())          
    
    def test_none_for_nan(self):     
        print('Er nå inne i test_none_for_nan')
        datetimes = pd.DataFrame({'datetimes': [datetime.now().date(),np.NaN]})
        new_datetimes = Oracle.none_for_nan(datetimes.copy())
        subresults = [
            new_datetimes.iloc[0,0] == datetime.now().date(),
            new_datetimes.iloc[1,0] is None
        ]
        self.assertTrue(pd.Series(subresults).all())
        
   
        
    
           
    # Må lage test som sjekker at 'col3' har like verdier som 'col5' og at
    #'col4' har like verdier oms 'col6' i de tilfeller der det ikke er null
    # Sjekker hvert element om det enten er missing eller er en "date"
    def test_datetime2date(self):
        print('Er nå inne i test_datetime2date')
        function_output = Oracle.datetime2date(df.copy())
        subresults = []
        for col in datetime_cols:
            for elem in function_output[col].to_list():
                subresults.append(
                    (isinstance(elem,pd._libs.tslibs.nattype.NaTType)) or 
             (isinstance(elem,date)))
        #            
        self.assertTrue(pd.Series(subresults).all())       
     
    def test_get_connection(self):
        print('Er nå inne i test_get_connection')
        conn = Oracle_Connect().get_connection()
        self.assertIsInstance(conn,oracledb.Connection)
        conn.close()
    #      
    
    def test_get_engine(self):
        print('Er nå inne i  test_get_engine')
        oracle_connect=Oracle_Connect()
        engine = oracle_connect.get_engine()
        self.assertIsInstance(engine,sqlalchemy.engine.base.Engine)    
        engine.dispose()
    
    def test_init_SQL_Generator(self):
        print('Er nå inne i test_init_SQL_Generator')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(df.dtypes,table_name=table_name)
        self.assertIsInstance(sql_generator,Oracle.SQL_Generator)
    
    def test_init_SQL_Create(self):
        print('Er nå inne i test_init_SQL_Create')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(df.dtypes,table_name=table_name)
        sql_create = Oracle.SQL_Create(sql_generator) 
        self.assertIsInstance(sql_create,Oracle.SQL_Create)
    
    def test_init_SQL_Insert(self):
        print('Er nå inne i test_init_SQL_Insert')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(df.dtypes,table_name=table_name)
        sql_insert = Oracle.SQL_Insert(sql_generator) 
        self.assertIsInstance(sql_insert,Oracle.SQL_Insert)      
    
    
    #************ test_prepare_data_to_insert Er nå en slags "visuell inspeksjonstest"
    # Må gjøres om til automatisk test
    def test_prepare_data_to_insert(self):
        print('Er nå inne i test_prepare_data_to_insert')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(df.dtypes,table_name=table_name)
        sql_insert = Oracle.SQL_Insert(sql_generator)
        list_data = sql_insert.prepare_data_to_insert(df.copy()) 
        print('list_data er')
        print(list_data)

    def test_sql_create_table(self):
        print('Er nå inne i test_sql_create_table')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(df.dtypes,table_name=table_name)
        sql_create = Oracle.SQL_Create(sql_generator)
        sql_create = sql_create.sql_create_table()
        sql_comparison = '''
        CREATE PRIVATE TEMPORARY TABLE ORA$PTT_MY_TABLE (
        "col0" NUMBER,
        "col1" VARCHAR2(255),
        "col2" INTEGER,
        "col3" DATE,
        "col4" DATE
        ) ON COMMIT PRESERVE DEFINITION        
        '''
        stripped_sql_create =  re.sub(sql_create.strip(),"\n"," ")
        stripped_sql_comparison =  re.sub(sql_comparison.strip(),"\n"," ")
        self.assertEqual(stripped_sql_create,stripped_sql_comparison)   
        
    def test_conditional_col_transform(self):
        print('Er nå inne i test_conditional_col_transform')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(df[['col1','col2','col3','col4']].dtypes,table_name=table_name)
        print('sql_generator.db_dtypes er ')
        print(sql_generator.db_dtypes)
        sql_insert = Oracle.SQL_Insert(sql_generator) 
        transformations = [sql_insert.conditional_col_transform(ind) for ind,_ in  enumerate(sql_generator.cols)] 
        for transformation in transformations:
            print(transformation)
        #d

    
    def test_create_or_replace_persistent_table(self):
        print('Er nå inne i test_create_or_replace_persistent_table')
        table_name = "MY_TEST_TABLE"
        oracle_connect=Oracle_Connect()
        print('Har nå klart å lage oracle_connect')
        oracle_connect.create_or_replace_persistent_table(df[['col1','col2','col3','col4']].copy(),table_name = "MY_TEST_TABLE")
        cx_conn = oracle_connect.get_connection()
        crsr = cx_conn.cursor()
        self.assertTrue(Oracle.exists_table(table_name,crsr))
        cx_conn.close()
        print('Har nå forhåpentlig klart testen test_create_or_replace_persistent_table')
        
        
      
if __name__ == '__main__':
    unittest.main()
