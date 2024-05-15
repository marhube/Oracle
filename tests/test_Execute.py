import os
import sys
import re
import pandas as pd
#**********
#********

#********* Start internimport
from TestData.TestData import generate_test_data
from Oracle.PrepData import PreprocessData
from Datatype.Datatype import find_date_columns
import Oracle.SQL as SQL
from Oracle.Oracle_Connect import Connect
from Oracle.Execute import Exec,TableOps
from Oracle.IO import ImportData
#
from tests.test_IO import compare_db_data_types


#********* Slutt internimport





import unittest

class TestExec(unittest.TestCase):
    
    def test_exists_user_table(self):
        print('Er nå inne i TestExec.test_exists_user_table som ikke er implementert') 
    
    def test_run_sql_from_file(self):
        print('Er nå inn i TestExport.test_run_sql_from_file')
        #Memo til selv: Tester ved å kjøre en sql-fil som kun lager "temp-tabeller". Dette
        # er for å begrense mengden avhengigheter av andre funksjoner
        sql_file_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/selfcontained_only_temp.sql"
        output_table = 'ORA$PTT_blabla'
        conn = Connect().get_connection()
        exec = Exec(conn)
        exec.run_sql_from_file(path_sql = sql_file_path)           
        #conn.
        table_exists = exec.exists_user_table(output_table)
        conn.close()
        self.assertTrue(table_exists) 
    
    
    
    def test_get_exact_table_name(self):
        print('Er nå inne i TestExec.test_get_exact_table_name(')
        #Lager først tabellen (uten å populere den)
        table_name =  "ORA$PTT_blabla_mixedCase"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True) 
        prep_data = PreprocessData(test_data.dtypes,table_name=table_name,uppercase = False)
        conn = Connect().get_connection()
        import_data = ImportData(prep_data,conn)
        import_data.create_or_replace_user_table()
        #
        exec = Exec(conn)
        exact_table_name = exec.get_exact_user_table_name(table_name)       
        conn.close()
        self.assertEqual(exact_table_name,table_name) 

     
    def test_get_list_of_all_user_tables(self):
        print('Er nå inne i test_get_list_of_all_user_tables som ikke er ferdig implementert')
        conn = Connect().get_connection()
        exec = Exec(conn)
        list_of_user_tables = exec.get_list_of_all_user_tables()
        conn.close()     
        #
        print(f'list_of_user_tables er {list_of_user_tables}')
         
     
     
     #Memo to self: Change to just check th
    
       

class TestTableOps(unittest.TestCase):
    
    def test_nrows(self):
        print('Er nå inne i TestTableOps.test_nrows')
        table_name = "ORA$PTT_MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        conn = Connect().get_connection()
        import_data = ImportData(preprocess,conn)
        import_data.create_or_replace_table_and_insert_data(test_data)
        #
        tableOps = TableOps(table_name = table_name,conn = conn)
        nrows = tableOps.nrows()
        conn.close()
        self.assertEqual(nrows,test_n)
    
    def test_get_data_types_of_user_table(self):
        print('Er nå inne i TestTableOps.test_get_data_types_of_user_table som ikke er ferdig implementert')
        #Tester nå med en persistent (fast) tabell og en midlertidig tabell
        #Lager først tabellen (uten å populere den)
        table_name = "ORA$PTT_MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        conn = Connect().get_connection()
        import_data = ImportData(preprocess,conn)
        import_data.create_or_replace_table_and_insert_data(test_data)
        #
        tableOps = TableOps(table_name = table_name,conn = conn)
        data_types = tableOps.get_data_types_of_user_table()
        data_types_comparison = preprocess.map2db_dtype()
        comparison_results = compare_db_data_types(data_types,data_types_comparison)
        subresults = list(comparison_results.values())
        self.assertTrue(pd.Series(subresults).all()) 
    
    def test_drop_user_table(self):
        print('Er nå inne i TestTableOps.test_drop_table')
        table_name = "ORA$PTT_MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        conn = Connect().get_connection()
        import_data = ImportData(preprocess,conn)
        import_data.create_or_replace_table_and_insert_data(test_data)
        #
        exec = Exec(conn)
        exists_before = exec.exists_user_table(table_name)
        print(f'exists_before er {exists_before}')
        tableOps = TableOps(table_name = table_name,conn = conn)
        tableOps.drop_user_table()
        exists_after = exec.exists_user_table(table_name)
        conn.close()
        print(f'exists_after er {exists_after}')
        subresults = [exists_before,not exists_after]      
        self.assertTrue(pd.Series(subresults).all())              
    
    def test_drop_user_table_if_exists(self):
        print('Er nå inne i TestTableOps.test_drop_user_table som ikke er i')
    
  
    
    
    

        
    
    
    
        
    
        
        
        