import os
import sys
import re
import pandas as pd
#**********
#********



#Forsøk på  systematisk testing
import oracledb
import Oracle.SQL as SQL
from Oracle.Oracle_Connect import Connect
from Oracle.Execute import Exec,TableOps,Export

import unittest

def run_if_not_exists(table_name: str,path_sql: str,conn: oracledb.Connection):
    print('Er nå inne i  run_if_not_exists')
    exec = Exec(conn)
    #Kjør opp tabell, hvis ikke finnes
    if not exec.exists_user_table(table_name):
        print('Er nå inne i if-en for å kjøre opp sql-koden')
        exec.run_sql_from_file(path_sql = path_sql)
    #

class TestExec(unittest.TestCase):
    
    def test_exists_user_table(self):
        print('Er nå inne i TestExec.test_exists_user_table som ikke er implementert') 
    
    def test_run_sql_from_file(self):
        print('Er nå inn i TestExport.test_run_sql_from_file')
        #Memo til selv: Tester ved å kjøre en sql-fil som kun lager "temp-tabeller". Dette
        # er for å begrense mengden avhengigheter
        sql_file_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/selfcontained_only_temp.sql"
        output_table = 'ORA$PTT_blabla'
        conn = Connect().get_connection()
        exec = Exec(conn)
        exec.run_sql_from_file(path_sql = sql_file_path)           
        #
        table_exists = exec.exists_user_table(output_table)
        conn.close()
        self.assertTrue(table_exists) 
    
    
    
    def test_get_exact_table_name(self):
        print('Er nå inne i TestExec.test_get_exact_table_name(')
        table_name =  "ORA$PTT_blabla_mixedcase"
        path_sql = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/selfcontained_temp_mixed_case.sql"
        conn = Connect().get_connection()
        exec = Exec(conn)
        exec.run_sql_from_file(path_sql = path_sql)
        exact_table_name = exec.get_exact_user_table_name(table_name)
        conn.close()
        self.assertEqual(exact_table_name,"ORA$PTT_blabla_mixedCase") 
    
     #Memo to self: Change to just check th
    
       

class TestTableOps(unittest.TestCase):
    
    def test_get_data_types_of_user_table(self):
        print('Er nå inne i TestTableOps.test_get_data_types_of_user_table som ikke er implementert')
        table_name = 'blaBla'
        sql_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/selfcontained.sql"
        conn = Connect().get_connection()
        #Must create the table if doesn't already exist
        run_if_not_exists(table_name,sql_path,conn)
        #
        tableOps = TableOps(table_name = table_name,conn = conn)
        data_types = tableOps.get_data_types_of_user_table()
        conn.close()
        print(f'data_types er {data_types}')
    
    def test_drop_user_table(self):
        print('Er nå inne i TestTableOps.test_drop_table')
        conn = Connect().get_connection()
        table_name = "BLABLA"
        sql_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/selfcontained.sql"
        #Kjør opp tabell, hvis ikke finnes
        run_if_not_exists(table_name,sql_path,conn)
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
    
    
    
  
    
    
    
class TestExport(unittest.TestCase):
    
    def test_get_table(self):
        print('Er nå inne i TestExport.test_get_table som ikke er implementert')  
        sql_file_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/selfcontained_only_temp.sql" 
        output_table = 'ORA$PTT_fodsel'
        conn = Connect().get_connection()
        exec = Exec(conn)
        exec.run_sql_from_file(path_sql= sql_file_path)         
        export = Export(table_name = output_table,conn=conn,clean_output = False) 
        #
        df = export.get_table()
        conn.close()
        subresults = [isinstance(df,pd.DataFrame),df.shape[0] == 100]
        self.assertTrue(pd.Series(subresults).all())  
    
    def test_clean_export(self):
        print('Er nå inne i TestExport.test_get_table som ikke er implementert')  
        output_table = "FODSEL"
        sql_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/selfcontained.sql"
        conn = Connect().get_connection()
        #Kjør opp tabell, hvis ikke finnes
        run_if_not_exists(output_table,sql_path,conn) 
        export = Export(table_name = output_table,conn=conn,clean_output = True) 
        #
        df = export.get_table()
        conn.close()
        print('df.head() er')
        print(df.head())
    
    
    
        
    #Memo to self: only works for persistent tables    
        
    
        
        
        