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
from Oracle.Datatype import get_data_type_from_values
from Oracle.PrepData import PreprocessData,datetime2date
from Oracle.Execute import drop_user_table_if_exists,OracleError,Exec,TableOps
#******* Slutt internimport


class ImportData(Exec): 
    #
    def __init__(self, 
                 preprocess: PreprocessData,
                 conn: oracledb.Connection
                 ):
        self.conn = conn
        self.preprocess = preprocess        
        self.table_name = preprocess.table_name
    
    
    #Memo to self: "set_table_name" is 
    def set_table_name(self) -> 'ImportData':
        print('Er nå inne i set_table_name')
        table_name = self.preprocess.table_name
        if super().exists_user_table(table_name):
            table_name = super().get_exact_user_table_name(table_name)
        
        self.preprocess.table_name = table_name       
        self.table_name = table_name
        return self
    
    
    def create_user_table(self) -> None:
        print('Er nå inne i SQL_Insert.create_table()')
        sql_create = SQL_Create(self.preprocess)
        str_create_table = sql_create.sql_create_table()
        print(f'str_create_table er {str_create_table}')
        super().exec_and_commit(str_create_table)
        
    def create_or_replace_user_table(self) -> None:
        print(f'Er nå inne i create_or_replace_user_table der self.table_name er {self.table_name}')
        drop_user_table_if_exists(self.table_name,self.conn)
        self.create_user_table()
    
    #Memo to self: table_name might be modified
    def insert_data(self,df: pd.DataFrame) -> 'ImportData':
        self.set_table_name()
        str_insert_into =  self.preprocess.sql_insert_into()
        data_to_insert = self.preprocess.preprocess_data(df=df)
        super().insert_data(str_insert_into, data_to_insert)
        return self
    
    def create_or_replace_table_and_insert_data(self,df: pd.DataFrame) -> 'ImportData':
        print('Er nå inne i create_or_replace_table_and_insert_data)')
        self.create_or_replace_user_table()
        self.insert_data(df)
        return self
    
    def create_table_if_not_exists_and_insert_data(self,df: pd.DataFrame) -> 'ImportData':
        if not super().exists_user_table(self.table_name):
            self.create_user_table()
        #
        self.insert_data(df)
        return self
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
    
    def get_table(self) -> pd.DataFrame:
        # Sjekker om 
        sql_output_table = SQL.generate_select(self.table_name)
        df = super().fetch_table(sql_output_table)
        if self.postprocess:
            if SQL.is_private_temp_table(self.table_name):
                raise OracleError("Table {self.table_name} is a private table and cannot be cleaned.")
                
            df = self.postprocess(df)
        return df
    #
        