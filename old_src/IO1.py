#******** Start type hinting
from typing import Optional
#******** Slutt type hinting

import os
import sys # For testing
#
import pandas as pd

#********** Start databaserelatert
import oracledb
#********** Slutt databaserelatert

#******* Start internimport
import Oracle.SQL as SQL
from Oracle.SQL import SQL_Create
from Oracle.PrepData import PreprocessData,datetime2date
from Oracle.Execute import drop_user_table_if_exists,OracleError,Exec,TableOps
#******* Slutt internimport


class ImportData(Exec): 
    #
    def __init__(self, 
                 preprocess: PreprocessData,
                 conn: oracledb.Connection
                 ):
        self.preprocess = preprocess
        self.table_name = super().get_exact_user_table_name(preprocess.table_name)        
        self.conn = conn
    
    def create_user_table(self) -> None:
        print('Er nå inne i SQL_Insert.create_table()')
        sql_create = SQL_Create(self.preprocess)
        str_create_table = sql_create.sql_create_table()
        print(f'str_create_table er {str_create_table}')
        super().exec_and_commit(str_create_table)
        
    def create_or_replace_user_table(self) -> None:
        drop_user_table_if_exists(self.table_name,self.conn)
        self.create_user_table()
    
    #OBSSSSSSSSS Må lage tester
    def insert_data(self,df: pd.DataFrame) -> None:
        str_insert_into =  self.preprocess.sql_insert_into()
        data_to_insert = self.preprocess.preprocess_data(df=df)
        super().insert_data(str_insert_into, data_to_insert)
    
    def create_or_replace_table_and_insert_data(self,df: pd.DataFrame) -> None:
        self.create_or_replace_user_table()
        self.insert_data(df)
    
    def create_table_if_not_exists_and_insert_data(self,df: pd.DataFrame) -> None:
        if not super.exists_user_table(self.table_name):
            self.create_user_table()
        #
        self.insert_data(df)
#    
    
    
class ExportData(Exec):
    def __init__(self, table_name : str, conn: oracledb.Connection,postprocess: bool =True):
        self.conn = conn
        self.table_name = super().get_exact_user_table_name(table_name)
        self.tableOps = TableOps(self.table_name,conn)
        self.postprocess = postprocess        
    
   
    def postprocess_date_values(self,date_values: pd.Series) -> pd.Series:
        date_values = datetime2date(date_values)
        return date_values
    
    # Todo: Change name to "postprocess_data"
    def postprocess(self,df: pd.DataFrame) -> pd.DataFrame:
        print('Er nå inne i Export.postprocess')
        #Innhenter hva som var opprinnelige datatypper 
        db_data_types: dict = self.tableOps.get_data_types_of_user_table()
        #Memo to self: Doesn't check if "date values" are datetimes (maybe I should)
        for col in db_data_types.keys():
            if db_data_types[col].upper() == "DATE":
                df[col] = datetime2date(df[col])
        #
        return df
    
    #Utility function "get_fetch_name"
    #Memo to self: When fetching data   "mixed case " table names need to be enclosed with ""
    def get_fetch_name(self) -> str:
        fetch_name = self.table_name
        if fetch_name not in [fetch_name.upper(),fetch_name.lower()]:
            fetch_name = f'"{fetch_name}"'
        return fetch_name

    def get_table(self) -> pd.DataFrame:
        fetch_name = self.get_fetch_name()
        # Sjekker om 
        sql_output_table = SQL.generate_select(fetch_name)
        df = super().fetch_table(sql_output_table)
        if self.clean_output:
            if SQL.is_private_temp_table(self.table_name):
                raise OracleError("Table {self.table_name} is a private table and cannot be cleaned.")
                
            df = self.clean_export(df)
        return df
    #
        