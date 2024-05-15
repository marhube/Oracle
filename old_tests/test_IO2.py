import os
import sys
#**********
import pandas as pd
import numpy as np
import datetime as dt
from math import floor
#********

#Forsøk på  systematisk testing


#********* Start internimport
from TestData import generate_test_data

from Oracle.SQL import SQL_Create
from Oracle.Datatype import robust_get_data_type_from_values
from Oracle.PrepData import PreprocessData
from Oracle.Oracle_Connect import Connect
from Oracle.Execute import drop_user_table_if_exists,Exec,TableOps
from Oracle.IO import ImportData

#********* Slutt internimport

import unittest


class TestIO(unittest.TestCase):
        
    def test_create_user_table(self):
        print('Er nå inne i TestIO.test_create_user_table')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        conn = Connect().get_connection()
        exec = Exec(conn)
        drop_user_table_if_exists(table_name = table_name, conn = conn)
        exists_before = exec.exists_user_table(table_name)
        prep_data = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        import_data = ImportData(prep_data,conn)             
        import_data.create_user_table()
        exists_after = exec.exists_user_table(table_name) 
        tableOps = TableOps(table_name,conn)
        nrows = tableOps.nrows()
        conn.close()
        subresults = [not exists_before,exists_after,nrows == 0]      
        self.assertTrue(pd.Series(subresults).all()) 
       
    
    def test_insert_data(self):
        print('Er nå inne i TestImport.test_create_or_replace_persistent_table som ikke er ferdig implementert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        #
        first_half = floor(test_n/2)
        test_data1 = test_data.iloc[0:first_half,:]
        test_data2 = test_data.iloc[first_half:test_n,:]
        #
        conn = Connect().get_connection()
        drop_user_table_if_exists(table_name = table_name, conn = conn)
        import_data = ImportData(preprocess,conn) 
        import_data.create_or_replace_table_and_insert_data(test_data1)
        tableOps = TableOps(table_name,conn)
        nrows1 = tableOps.nrows()
        import_data.insert_data(test_data2)
        nrows2 = tableOps.nrows()
        conn.close()
        #
        

        
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
    
    #Memo til selv: Må bytte ut denne testem med å teste på egenprodusert (og importert) data!!!
    # nr
    def test_clean_export(self):
        print('Er nå inne i TestExport.test_get_table som ikke er implementert')  
        output_table = "BLABLA"
        print(f'output_table er {output_table}')
        sql_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/selfcontained.sql"
        #Lager "fasitdatatyper" manuelt
        datatype_comparison = {}
        datatype_comparison['SEKVENSNUMMER'] = 'int'
        datatype_comparison['RAD'] = 'int'
        datatype_comparison['GYLDIGHETSTIDSPUNKT'] = 'pandas._libs.tslibs.timestamps.Timestamp'
        datatype_comparison['AJOURHOLDSTIDSPUNKT'] = 'pandas._libs.tslibs.timestamps.Timestamp'
        datatype_comparison['FOEDSELSDATO'] = 'datetime.date'
        datatype_comparison['ERGJELDENDE'] = 'str'
        
        conn = Connect().get_connection()
        #Kjør opp tabell, hvis ikke finnes
        # Sammenligne med resultat uten
        exec = Exec(conn)
        exec.run_if_not_exists(output_table,sql_path)
        export = Export(table_name = output_table,conn=conn,clean_output = True) 
        print(f'tilbake i test_clean_export så er export.table_name {export.table_name}')
        #
        df = export.get_table()        
        conn.close()
        print('df.head() er')
        print(df.head())
        subresults = []
        for col in datatype_comparison.keys():
            next_subresult = False
            if col in df.columns:
                next_subresult = (get_data_type_from_values(df[col]) == datatype_comparison[col])
            #
            subresults.append(next_subresult)
        #
        self.assertTrue(pd.Series(subresults).all())         
    

        
    