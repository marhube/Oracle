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
import Oracle.PrepData as PrepData


#******* Slutt internimport


class TableOps:
    def __init__(self, table_name: str, conn: oracledb.Connection):
        self.table_name = table_name
        self.conn = conn
        self.crsr = self.conn.cursor()

    def retrieve_exact_table_name(self):
        print('Er nå inne i retrieve_exact_table_name')
        str_retrieve_exact_table_name = SQL.sql_search_user_table(self.table_name)
        print(f'str_retrieve_exact_table_name er {str_retrieve_exact_table_name}')
        crsr = self.conn.cursor()
        fetcher = Fetcher(table_name = self.table_name,crsr = crsr)
        exact_table_name = fetcher.fetch_table(str_retrieve_exact_table_name).iloc[0,0]
        return exact_table_name
        

    def drop_table(self):
        print('Er nå inne i drop_table')
        str_drop_user_table = SQL.sql_drop_user_table(self.table_name)
        print(f'str_drop_user_table er {str_drop_user_table}')
        crsr = self.conn.cursor()
        crsr.execute(str_drop_user_table)
        self.conn.commit()

    def drop_user_table_if_exists(self):
        crsr = self.conn.cursor()
        fetcher = Fetcher(table_name = self.table_name,crsr = crsr)
        # Sjekk om tabellen finnes
        if fetcher.exists_user_table(self.table_name):
            self.drop_table(self.table_name)
    #            

        
        

class Fetcher:
    def __init__(self, table_name: str, conn: oracledb.Connection):
        tableOps = TableOps(table_name = table_name,)
        retrieve_exact_table_name(self)
        
        self.table_name = table_name
        self.conn = conn
        self.crsr = crsr
    
        

    def fetch_table(self,table_query) ->  pd.DataFrame:
        self.crsr.execute(table_query)        
        rows = self.crsr.fetchall()
        columns = [column[0] for column in self.crsr.description]
        df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
        return df   
  
    def exists_user_table(self) -> bool:        
        str_search_for_table  =  SQL.sql_search_user_table(self.table_name)
        print(f'str_search_for_table er {str_search_for_table}')
        df = self.fetch_table(str_search_for_table)
        return df.shape[0] > 0


    def get_data_types_of_user_table(self) -> pd.DataFrame:
        print('Er nå inne i retrieve_data_types_of_user_table')
        str_sql_data_types_user_table = SQL.sql_data_types_user_table(self.table_name)
        print(f'str_sql_data_types_user_table er {str_sql_data_types_user_table}.')
        df = self.fetch_table(str_sql_data_types_user_table)
        return df



    



class Export:
    def __init__(self, crsr: oracledb.Cursor, clean_output: bool =True):
        self.crsr = crsr
        self.clean_output = clean_output
    
    def clean_export(self,df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        print('Er nå inne i clean_export')
        fetcher = Fetcher(table_name = table_name, crsr = self.crsr)
        #Innhenter hva som var opprinnelige datatypper 
        db_data_types = fetcher.get_data_types_of_user_table()
        print(f'db_data_types er {db_data_types}')
        sys.exit()
    
    def get_table(self, table_name: str) -> pd.DataFrame:
        sql_output_table = SQL.generate_select(table_name)
        self.crsr.execute(sql_output_table)
        rows = self.crsr.fetchall()
        columns = [column[0] for column in self.crsr.description]
        df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
        if self.clean_output:
            df = self.clean_export(df=df,table_name=table_name)
        return df
    #
    
    # Kjører Oracle-sql kode
    def run_sql_from_file(self,path_sql: str,sql_query=None,output_table = None) -> Optional[pd.DataFrame]: 
        print(f'Er nå inne i run_sql_from_file der sql-koden som skal kjøres er fra {path_sql}')       
        parsed_sql_query = Parser(path_sql,sql_query=sql_query).parse_sql_code()
        print('parsed_sql_query er')
        print(parsed_sql_query)
        #
        for statement in parsed_sql_query: 
            self.crsr.execute(statement)
            # Commit the transaction 
            self.crsr.connection.commit()
        #
        print('Klarte forhåpentlig å gjennomføre hele kjøringen')
        
        #Hvis ønskelig så hentes en tabell ut
        df = None
        if output_table is not None:
            df = self.get_table(output_table)  
            
        print(f'Før avlevering fra run_sql_from_file så er df.__class__ {df.__class__}')      
        return df
    
    
        
    