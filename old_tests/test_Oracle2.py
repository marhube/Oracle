import unittest
import oracledb
from Oracle.Oracle import Oracle_Connect
#
class TestOracle(unittest.TestCase):
    def test_get_connection(self):
        oracle_connect=Oracle_Connect()
        conn = oracle_connect.get_connection()
        self.assertIsInstance(conn,oracledb.Connection)
        conn.close()
        # Fortsette med assertIsInstance
    #   self.assertEqual(hello_world(), "Hello, world!")
        
    def test_get_dsn(self):
        oracle_connect = Oracle_Connect()
        dsn_tns = oracle_connect.get_dsn()
        valid_dsn_string = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=rsl0folk-ref-analyse-db01)(PORT=1521))(CONNECT_DATA=(SID=rfolref1)))"
        self.assertEqual(dsn_tns,valid_dsn_string)        
    #
    
    def test_get_connection_string(self):
        oracle_connect = Oracle_Connect()  
        conn = oracledb.connect(
            user = oracle_connect.get_user(),
            password = oracle_connect.get_password(),
            dsn=dsn
        )
        
              
        connection_string = oracle_connect.get_connection_string()
        # Bør egentlig testes med at det går an å connecte
        print(f'connection_string er {connection_string}')
        self.assertIsInstance(connection_string ,str)

if __name__ == '__main__':
    unittest.main()
