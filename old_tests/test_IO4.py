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

from Oracle.Datatype import find_date_columns,get_data_type_from_values
from Oracle.PrepData import PreprocessData
from Oracle.Oracle_Connect import Connect
from Oracle.Execute import drop_user_table_if_exists,Exec,TableOps
from Oracle.IO import ImportData,ExportData

#********* Slutt internimport

import unittest


def compare_data_types(data_types_dict,data_types_comparison_dict):
    subresults_dict = {}
    for col in data_types_dict:
        if data_types_comparison_dict[col] == 'INTEGER':
            subresults_dict[col] = (data_types_dict[col] in ['INTEGER','NUMBER(0)'])
        else:
            subresults_dict[col] = (data_types_dict[col].startswith(data_types_comparison_dict[col]))
        #
    #    
    return subresults_dict
        



class TestIO(unittest.TestCase):
        
    #Memo til selv: Bør utvide test til også å sjekke datatyper
    def test_create_user_table(self):
        print('Er nå inne i TestIO.test_create_user_table')
        #table_name = "ORA$PTT_MY_TABLE"
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
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
        # Sjekker også datatype
        comparison_data_types = prep_data.map2db_dtype()
        data_types = tableOps.get_data_types_of_user_table()
        #Rydd til slutt opp
        tableOps.drop_user_table()
        conn.close()
        subresults = [not exists_before,exists_after,nrows == 0,len(comparison_data_types) == len(data_types)]
        #Sammenligner datatyper i database med hva det er meningen at det skal være
        data_type_comparisons = compare_data_types(data_types,comparison_data_types)
        for col in data_type_comparisons.keys():
            subresults.append(data_type_comparisons[col])
        #    
        self.assertTrue(pd.Series(subresults).all()) 
       
    
    def test_insert_data(self):
        print('Er nå inne i TestImport.test_insert_data')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        comparison_data_types = preprocess.map2db_dtype()
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
        #sammenligner datatyper
        data_types = tableOps.get_data_types_of_user_table()  
        #
        #Rydd til slutt opp
        tableOps.drop_user_table()
        conn.close()
        #
        subresults = [nrows1 ==first_half ,nrows2 == test_n]  
        #sammenligner datatyper
        data_type_comparisons = compare_data_types(data_types,comparison_data_types)
        for col in data_type_comparisons.keys():
            subresults.append(data_type_comparisons[col])
        #    
        self.assertTrue(pd.Series(subresults).all()) 
        

        
class TestExport(unittest.TestCase):
    
    def test_get_table(self):
        print('Er nå inne i TestExport.test_get_table som ikke er ferdig implementert')  
        #Memo to self: First need to import some data
        table_name = "ORA$PTT_MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        date_cols = find_date_columns(test_data)
        date_cols.append('date_only_na')
        preprocess = PreprocessData(test_data.dtypes,table_name=table_name,date_cols=date_cols)
        #
        conn = Connect().get_connection()
        import_data = ImportData(preprocess,conn)
        import_data.create_or_replace_table_and_insert_data(test_data)
        #      table_name : str, conn: oracledb.Connection,postprocess: bool =True
        export_data = ExportData(table_name = table_name,conn=conn,postprocess_data = False) 
        #
        df = export_data.get_table()
        conn.close()
        subresults = [isinstance(df,pd.DataFrame),df.shape[0] == test_n]
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
        export_data = ExportData(table_name = output_table,conn=conn,clean_output = True) 
        print(f'tilbake i test_clean_export så er export.table_name {export_data.table_name}')
        #
        df = export_data.get_table()        
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
    

        
    