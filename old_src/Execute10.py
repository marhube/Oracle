#******** Start type hinting
from typing import Optional
#******** Slutt type hinting

import os
import sys # For testing
#
import pandas as pd
import numpy as np
#********** Start databaserelatert
import oracledb
#********** Slutt databaserelatert

#******* Start internimport

import Oracle.SQL as SQL
from Oracle.SQL import Parser

#******* Slutt internimport


class OracleError(Exception):
    """Custom exception class."""
    def __init__(self, message):
        super().__init__(message)


#Memo to self: "Exec" doesn't require  any tables to exist
class Exec:
    def __init__(self, conn: oracledb.Connection):
        self.conn = conn
    
    def exec_and_commit(self,query: str) -> None:
        crsr = self.conn.cursor()
        crsr.execute(query)
        self.conn.commit()
        crsr.close()
    
    def insert_data(self,sql_insert: str, data_to_insert: list) -> None:
        crsr = self.conn.cursor()
        crsr.executemany(sql_insert,data_to_insert)
        self.conn.commit()
        crsr.close()
        
 
    def fetch_table(self,table_query: str) ->  pd.DataFrame:
        crsr = self.conn.cursor()
        crsr.execute(table_query)        
        rows = crsr.fetchall()
        columns = [column[0] for column in crsr.description]
        crsr.close()
        df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
        return df

    def exists_user_table(self,table_name: str) -> bool:        
        str_search_for_table  =  SQL.sql_search_user_table(table_name)
        print(f'str_search_for_table er {str_search_for_table}')
        df = self.fetch_table(str_search_for_table)
               
        return df.shape[0] > 0
         
    
    def get_exact_user_table_name(self,table_name: str) -> str:
        print('Er nå inne i get_exact_user_table_name')
        if not self.exists_user_table(table_name):
            raise OracleError(f"Table {table_name} does not exist")
        #
        str_get_exact_table_name = SQL.sql_search_user_table(table_name)
        #Memo to self: Need to enclose with "str()" for mypy to accept that 
        #exact_tbale_names actually is a string        
        exact_table_name = self.fetch_table(str_get_exact_table_name).iloc[0,0]
        if not isinstance(exact_table_name,str):
            raise OracleError("Fetch result is not a string")
        return exact_table_name
    
    # Kjører Oracle-sql kode
    def run_sql_from_file(self,path_sql: str,sql_query=None) -> None: 
        print(f'Er nå inne i run_sql_from_file der sql-koden som skal kjøres er fra {path_sql}')       
        parsed_sql_query = Parser(path_sql,sql_query=sql_query).parse_sql_code()
        print('parsed_sql_query er')
        print(parsed_sql_query)
        ## Commit the transaction 
        for statement in parsed_sql_query: 
            self.exec_and_commit(statement)
         #
        print('Klarte forhåpentlig å gjennomføre hele kjøringen')
        
    def run_if_not_exists(self,table_name: str,path_sql: str):
        #Kjør opp tabell, hvis ikke finnes
        if not self.exists_user_table(table_name):
            self.run_sql_from_file(path_sql = path_sql)
        #

    def get_list_of_tables(self,sql_query) -> list:
        list_of_tables_as_frame = self.fetch_table(sql_query)
        list_of_tables = list_of_tables_as_frame['TABLE_NAME'].tolist()
        return list_of_tables
    
    def get_list_of_persistent_user_tables(self) -> list:
        sql_list_user_tables = SQL.sql_list_user_tables()
        list_of_tables = self.get_list_of_tables(sql_list_user_tables)
        return list_of_tables
    
    def get_list_of_temporary_user_tables(self) -> list:
        sql_list_tmporary_user_tables = SQL.sql_list_user_private_temp_tables()
        list_of_tables = self.get_list_of_tables(sql_list_tmporary_user_tables)
        return list_of_tables
    
    
    def get_list_of_all_user_tables(self) -> list:
        list_all_user_tables = self.get_list_of_persistent_user_tables()
        list_all_user_tables.extend(self.get_list_of_temporary_user_tables())
        return list_all_user_tables


# Memo to self: The class "TableOps" requires the table "table_name" to exist
class TableOps(Exec):
    def __init__(self, table_name: str, conn: oracledb.Connection):
        self.conn = conn
        self.table_name = super().get_exact_user_table_name(table_name)       
    
    def nrows(self) -> int:
        print('Er nå inne i TableOps.nrows')
        str_sql_nrows = SQL.sql_nrows(self.table_name)
        print(f'str_sql_nrows er {str_sql_nrows}')
        nrows_frame = super().fetch_table(str_sql_nrows)
        #
        nrows_int =  nrows_frame.iloc[0,0]
        print(f'nrows_int.__class__ er {nrows_int.__class__} og nrows_int er {nrows_int}')
        #
        if isinstance(nrows_int,int) or isinstance(nrows_int,np.int64):
            nrows_int = int(nrows_int)
        else:
            print(f'nrows_int er {nrows_int}')
            raise OracleError(f" Output from query {str_sql_nrows} is not an integer.")
        #
            
        return nrows_int    
     
     
        
    def get_data_types_of_persistent_user_table(self) -> dict:
        print('Er nå inne i TableOps.get_data_types_of_persistent_user_table')
        str_sql_data_types_user_table = SQL.sql_data_types_user_table(self.table_name)
        print(f'str_sql_data_types_user_table er {str_sql_data_types_user_table}.')
        db_data_types_frame = super().fetch_table(str_sql_data_types_user_table)
        db_data_types_dict = dict(zip(db_data_types_frame.iloc[:,0], db_data_types_frame.iloc[:,1]))
        return db_data_types_dict
    
    
    # workaround utility function 
    def create_representation_table(self,replace_name) -> dict:
        sql_create_representation_table = SQL.sql_create_replace_table(replace_name,self.table_name)
        super().exec_and_commit(sql_create_representation_table)
        str_sql_data_types_user_table = SQL.sql_data_types_user_table(replace_name)
        db_data_types_frame = super().fetch_table(str_sql_data_types_user_table)
        db_data_types_dict = dict(zip(db_data_types_frame.iloc[:,0], db_data_types_frame.iloc[:,1]))
        return db_data_types_dict
    
    # workaround utility function   
    def get_data_types_of_private_table(self) -> dict:
        print('Er nå inne i TableOps.get_data_types_of_private_table')
        replace_name = self.table_name.upper().replace("_","$")        
        db_data_types_dict = self.create_representation_table(replace_name)        
        str_drop_user_table = SQL.sql_drop_user_table(replace_name)
        #Finally delete table
        super().exec_and_commit(str_drop_user_table)
        return db_data_types_dict
    
    def get_data_types_of_user_table(self) -> dict:
        print('Er nå inne i TableOps.get_data_types_of_user_table')
        # Only works for persistent tables
        db_data_types_dict = {}
        if SQL.is_private_temp_table(self.table_name):
            db_data_types_dict = self.get_data_types_of_private_table()
        else:
            db_data_types_dict = self.get_data_types_of_persistent_user_table()
        return db_data_types_dict

    
    
    def drop_user_table(self) -> None:
        print('Er nå inne i TableOps.drop_user_table')
        str_drop_user_table = SQL.sql_drop_user_table(self.table_name)
        print(f'str_drop_user_table er {str_drop_user_table}')
        super().exec_and_commit(str_drop_user_table)
    # 
    
    def insert_data(self,str_insert_into : str, data_to_insert: list) -> None:
        super().insert_data(str_insert_into, data_to_insert)
               

def drop_user_table_if_exists(table_name: str,conn: oracledb.Connection) -> None:
    # Sjekk om tabellen finnes
    exec = Exec(conn)
    if exec.exists_user_table(table_name):
        tableOps = TableOps(table_name = table_name,conn=conn)
        tableOps.drop_user_table()
    #    




    
   
       
        
        
    
        
    