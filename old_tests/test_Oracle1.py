import unittest
import oracledb
from Oracle.Oracle import Oracle_Connect
#
class TestOracle(unittest.TestCase):
    def test_get_connection(self):
        print('Kommer inn i test_get_connection')
        oracle_connect=Oracle_Connect()
        print(f'type(oracle_connect) er {type(oracle_connect)}')
        cx_conn = oracle_connect.get_connection()
        print(f'type(cx_conn) er {type(cx_conn)}')
        self.assertIsInstance(cx_conn,oracledb.Connection)
        print('Har også kjørt  self.assertIsInstance')
        cx_conn.close()
        # Fortsette med assertIsInstance
    #   self.assertEqual(hello_world(), "Hello, world!")

if __name__ == '__main__':
    unittest.main()
