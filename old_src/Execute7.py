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
            raise OracleError("Table {table_name} does not exist")
        #
        str_get_exact_table_name = SQL.sql_search_user_table(table_name)
        print(f'str_retrieve_exact_table_name er {str_get_exact_table_name}')
        result = self.fetch_table(str_get_exact_table_name)
        print(f'result er {result}')        
        exact_table_name = self.fetch_table(str_get_exact_table_name).iloc[0,0]
        return exact_table_name
    
    # Kjører Oracle-sql kode
    def run_sql_from_file(self,path_sql: str,sql_query=None): 
        print(f'Er nå inne i run_sql_from_file der sql-koden som skal kjøres er fra {path_sql}')       
        parsed_sql_query = Parser(path_sql,sql_query=sql_query).parse_sql_code()
        print('parsed_sql_query er')
        print(parsed_sql_query)
        crsr = self.conn.cursor()
        #
        for statement in parsed_sql_query: 
            crsr.execute(statement)
            # Commit the transaction 
            self.conn.commit()
        #
        crsr.close()
        print('Klarte forhåpentlig å gjennomføre hele kjøringen')
        
    def run_if_not_exists(self,table_name: str,path_sql: str):
        #Kjør opp tabell, hvis ikke finnes
        if not self.exists_user_table(table_name):
            self.run_sql_from_file(path_sql = path_sql)

        #


# Memo to self: The class "TableOps" requires the table "table_name" to exist
class TableOps(Exec):
    def __init__(self, table_name: str, conn: oracledb.Connection):
        self.conn = conn
        self.table_name = super().get_exact_user_table_name(table_name)       
    
      
    def get_data_types_of_user_table(self) -> pd.DataFrame:
        print('Er nå inne i get_data_types_of_user_table')
        str_sql_data_types_user_table = SQL.sql_data_types_user_table(self.table_name)
        print(f'str_sql_data_types_user_table er {str_sql_data_types_user_table}.')
        db_data_types_frame = super().fetch_table(str_sql_data_types_user_table)
        db_data_types_dict = dict(zip(db_data_types_frame.iloc[:,0], db_data_types_frame.iloc[:,1]))
        return db_data_types_dict

    def drop_user_table(self):
        print('Er nå inne i drop_table')
        str_drop_user_table = SQL.sql_drop_user_table(self.table_name)
        print(f'str_drop_user_table er {str_drop_user_table}')
        crsr = self.conn.cursor()
        crsr.execute(str_drop_user_table)
        self.conn.commit()
        crsr.close()

    def drop_user_table_if_exists(self):
        # Sjekk om tabellen finnes
        if super().exists_user_table():
            self.drop_table(self.table_name)
    #            




        
class Export(Exec):
    def __init__(self, table_name : str, conn: oracledb.Connection,clean_output: bool =True):
        self.conn = conn
        self.table_name = super().get_exact_user_table_name(table_name)
        self.tableOps = TableOps(self.table_name,conn)
        self.clean_output = clean_output        
      
  
    def clean_export(self,df: pd.DataFrame) -> pd.DataFrame:
        print('Er nå inne i clean_export')
        #Innhenter hva som var opprinnelige datatypper 
        db_data_types: dict = self.tableOps.get_data_types_of_user_table()
        #
        for col in db_data_types.keys():
            if db_data_types[col].upper() == "DATE":
                df[col] = df[col].map(lambda x: x.date()) 
        #
        return df
        
    
    def get_table(self) -> pd.DataFrame:
        sql_output_table = SQL.generate_select(self.table_name)
        df = super().fetch_table(sql_output_table)
        if self.clean_output:
            if SQL.is_private_temp_table(self.table_name):
                raise OracleError("Table {self.table_name} is a private table and cannot be cleaned.")
                
            df = self.clean_export()
        return df
    #
    
   
       
        
        #Hvis ønskelig så hentes en tabell ut
        df = None
        if output_table is not None:
            df = self.get_table(output_table)  
            
        print(f'Før avlevering fra run_sql_from_file så er df.__class__ {df.__class__}')      
        return df
    
    
        
    