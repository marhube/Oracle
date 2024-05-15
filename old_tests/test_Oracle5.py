import os
import sys
import re
#Forsøk på  systematisk testing
import unittest
import oracledb
import sqlalchemy
import datetimes as dt
from Oracle import Oracle 
from Oracle.Oracle import Oracle_Connect

#
#*********** Lager noe testdata som inkluderer 

data = {'col1': ['ABC', 'Abc', '123', '123', '12'],
                'col2': ['1', 'aB', '!!!' , '123', '234'],
                'col3': [1, 2, 3, 44, 55],
                'col4': [dt.datetime.now(),dt.datetime(2019, 12, 15, 13, 56, 3),
                         dt.datetime(2021,11,0,13,56,3), 'GroupB', 'GroupA']}
        
df = pd.DataFrame(data)


class TestOracle(unittest.TestCase):
    def test_replace_env_variables(self):
        string_with_variables = "Path is: ${PATH}, and home is: ${HOME}"
        result = Oracle.replace_env_variables(string_with_variables)
        self.assertIsInstance(result,str) 
    
    def test_generate_select(self):
        table_name = 'MY_TABLE'
        select_table = Oracle.generate_select(table_name)
        self.assertIsInstance(select_table,str)
        self.assertEqual(select_table,f'SELECT *  FROM {table_name}')
        
    
    
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
