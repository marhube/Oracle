import os
import sys
import re
#
import unittest
import oracledb
import sqlalchemy
from Oracle import Oracle 
from Oracle.Oracle import Oracle_Connect
#
class TestOracle(unittest.TestCase):
    def test_get_connection(self):
        conn = Oracle_Connect().get_connection()
        self.assertIsInstance(conn,oracledb.Connection)
        conn.close()
    
    # 
    def test_replace_env_variables(self):
        string_with_variables = "Path is: ${PATH}, and home is: ${HOME}"
        result = Oracle.replace_env_variables(string_with_variables)
        self.assertIsInstance(result,str)      
    
    def test_get_engine(self):
        oracle_connect=Oracle_Connect()
        engine = oracle_connect.get_engine()
        self.assertIsInstance(engine,sqlalchemy.engine.base.Engine)    
        engine.dispose()
        
      
if __name__ == '__main__':
    unittest.main()
