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
test_n = 10
test_data = generate_test_data(test_n)
#*********** Slutt Lager testdata
print(f'test_data er {test_data}')

class TestOracle(unittest.TestCase):    
    def test_generate_select(self):
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
        self.assertEqual(
            Oracle.extract_class_name("<class 'numpy.int64'>"),'numpy.int64'
            )
    
    
    def test_get_data_type_from_values(self):
        data_types = [Oracle.get_data_type_from_values(test_data[col]) for col in test_data.columns]
        print('data_types er')
        print(data_types)
        sys.exit()
    
    def test_get_class_from_dtypes(self):
        print('Er nå inne i test_get_class_from_dtypes')
        class_dict = Oracle.get_classes_from_dtypes(test_data.dtypes)
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
        print('Er nå inne i test_none_for_nan')
        datetimes = pd.DataFrame({'datetimes': [datetime.now().date(),np.NaN]})
        new_datetimes = Oracle.none_for_nan(datetimes.copy())
        subresults = [
            new_datetimes.iloc[0,0] == datetime.now().date(),
            new_datetimes.iloc[1,0] is None
        ]
        self.assertTrue(pd.Series(subresults).all())
        
    def test_date2datetime(self):
        function_output = Oracle.date2datetime(test_data.copy())
        subresults = []
        for col in ['date_with_na','date_no_na']:
            for elem in function_output[col].to_list():
                subresults.append(
                    (isinstance(elem,pd._libs.tslibs.nattype.NaTType)) or 
             (isinstance(elem,dt.datetime)))
        #            
        self.assertTrue(pd.Series(subresults).all())       
        
        
    
           
    # Må lage test som sjekker at 'col3' har like verdier som 'col5' og at
    #'col4' har like verdier oms 'col6' i de tilfeller der det ikke er null
    # Sjekker hvert element om det enten er missing eller er en "date"
    def test_datetime2date(self):
        print('Er nå inne i test_datetime2date')
        function_output = Oracle.datetime2date(test_data.copy())
        subresults = []
        for col in ['datetime_with_na','datetime_no_na']:
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
        sql_generator = Oracle.SQL_Generator(test_data.dtypes,table_name=table_name)
        self.assertIsInstance(sql_generator,Oracle.SQL_Generator)
    
    def test_init_SQL_Create(self):
        print('Er nå inne i test_init_SQL_Create')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(test_data.dtypes,table_name=table_name)
        sql_create = Oracle.SQL_Create(sql_generator) 
        self.assertIsInstance(sql_create,Oracle.SQL_Create)
    
    def test_init_SQL_Insert(self):
        print('Er nå inne i test_init_SQL_Insert')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(test_data.dtypes,table_name=table_name)
        sql_insert = Oracle.SQL_Insert(sql_generator) 
        self.assertIsInstance(sql_insert,Oracle.SQL_Insert)      
    
    
    #************ test_prepare_data_to_insert Er nå en slags "visuell inspeksjonstest"
    # Må gjøres om til automatisk test
    def test_prepare_data_to_insert(self):
        print('Er nå inne i test_prepare_data_to_insert')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(test_data.dtypes,table_name=table_name)
        sql_insert = Oracle.SQL_Insert(sql_generator)
        list_data = sql_insert.prepare_data_to_insert(test_data.copy()) 
        print('list_data er')
        print(list_data)

    def test_sql_create_table(self):
        print('Er nå inne i test_sql_create_table')
        table_name = "MY_TABLE"
        sql_generator = Oracle.SQL_Generator(test_data.dtypes,table_name=table_name)
        sql_create = Oracle.SQL_Create(sql_generator)
        sql_create_table = sql_create.sql_create_table()
        print('sql_create_table er')
        print(sql_create_table)
        
        sys.exit()
        sql_comparison = '''
        CREATE PRIVATE TEMPORARY TABLE ORA$PTT_MY_TABLE (
        "float_with_na" NUMBER,
        "float_no_na" NUMBER,
        "int_with_na" INTEGER,
        "int_no_na" INTEGER,
        "datetime_with_na" DATE,
        "datetime_no_na" DATE,
        
        
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
        sql_generator = Oracle.SQL_Generator(test_data.dtypes,table_name=table_name)
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
        oracle_connect.create_or_replace_persistent_table(test_data.copy(),table_name = "MY_TEST_TABLE")
        cx_conn = oracle_connect.get_connection()
        crsr = cx_conn.cursor()
        self.assertTrue(Oracle.exists_table(table_name,crsr))
        cx_conn.close()
        print('Har nå forhåpentlig klart testen test_create_or_replace_persistent_table')
        
        
      
if __name__ == '__main__':
    unittest.main()
