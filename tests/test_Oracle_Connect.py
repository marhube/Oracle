import os
import sys
import re

#**********
import oracledb
import sqlalchemy
#********

#Forsøk på  systematisk testing
from Oracle.Oracle_Connect import replace_env_variables,Connect

import unittest

class TestOracleConnect(unittest.TestCase):
    
    def test_replace_env_variables(self):
        print('Er nå inne i TestOracle.test_replace_env_variables')        
        string_with_variables = "Path is: ${PATH}, and home is: ${HOME}"
        result = replace_env_variables(string_with_variables)
        self.assertIsInstance(result,str)
    
    
    def test_get_connection(self):
        print('Er nå inne i TestOracleConnect.test_get_connection')
        conn = Connect().get_connection()
        self.assertIsInstance(conn,oracledb.Connection)
        conn.close()
    #
    
    def test_get_engine(self):
        print('Er nå inne i  TestOracleConnect.test_get_engine')
        connect = Connect()
        engine = connect.get_engine()
        self.assertIsInstance(engine,sqlalchemy.engine.base.Engine)    
        engine.dispose() 
        
        
    