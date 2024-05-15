import os
import sys
import re
#
import unittest
import oracledb
import sqlalchemy
from Oracle.Oracle import Oracle_Connect
#
# Hjelpefunksjon for å kunne teste "connection_string"
def replace_env_variables(s):
    # This function will find all occurrences of ${VAR_NAME} in the string
    # and replace them with the value of the environment variable VAR_NAME.
    
    # Define a pattern to match ${ANYTHING_HERE}
    pattern = re.compile(r'\$\{(.+?)\}')

    # Replace each found pattern with the corresponding environment variable
    def replace(match):
        var_name = match.group(1)  # Extract variable name
        return os.environ.get(var_name, '')  # Replace with env variable value

    return pattern.sub(replace, s)

class TestOracle(unittest.TestCase):
    def test_get_connection(self):
        oracle_connect=Oracle_Connect()
        conn = oracle_connect.get_connection()
        self.assertIsInstance(conn,oracledb.Connection)
        conn.close()
    
    def test_get_connection_string(self):
        oracle_connect=Oracle_Connect()
        connection_string = oracle_connect.get_connection_string()
        print(f'connection_string er først{connection_string} og så {replace_env_variables(connection_string)} ')
        sys.exit()
        engine = sqlalchemy.create_engine(replace_env_variables(connection_string))
        print(f'type(engine) er {type(engine)}')
        #self.assertIsInstance(conn.oracledb.Connection)
        #conn.close()
        
      
if __name__ == '__main__':
    unittest.main()
