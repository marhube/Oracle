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
#Memo til selv: "Exec er uavhengig av "
class Exec:
    def __init__(self, conn: oracledb.Connection):
        self.conn = conn
    
    def fetch_table(self,table_query) ->  pd.DataFrame:
        crsr = self.conn.cursor()
        crsr.execute(table_query)        
        rows = crsr.fetchall()
        columns = [column[0] for column in crsr.description]
        crsr.close()
        df = pd.DataFrame((tuple(row) for row in rows), umns=columns)
        return df

    

class TableOps:
    def __init__(self, table_name: str, conn: oracledb.Connection):
        self.orig_table_name = table_name
        self.conn = conn
        self.table_name = self.get_exact_table_name()
    
    def fetch_table(self,table_query) ->  pd.DataFrame:
        crsr = self.conn.cursor()
        crsr.execute(table_query)        
        rows = crsr.fetchall()
        columns = [column[0] for column in crsr.description]
        crsr.close()
        df = pd.DataFrame((tuple(row) for row in rows), umns=columns)
        return df 

    def exists_user_table(self) -> bool:        
        str_search_for_table  =  SQL.sql_search_user_table(self.table_name)
        print(f'str_search_for_table er {str_search_for_table}')
        df = self.fetch_table(str_search_for_table)
        return df.shape[0] > 0

    def get_exact_table_name(self):
        print('Er nå inne i get_exact_table_name')
        str_get_exact_table_name = SQL.sql_search_user_table(self.orig_table_name)
        print(f'str_retrieve_exact_table_name er {str_get_exact_table_name}')
        result = self.fetch_table(str_get_exact_table_name)
        print(f'result er {result}')
        exact_table_name = self.fetch_table(str_get_exact_table_name).iloc[0,0]
        return exact_table_name
        
    def get_data_types_of_user_table(self) -> pd.DataFrame:
        print('Er nå inne i get_data_types_of_user_table')
        str_sql_data_types_user_table = SQL.sql_data_types_user_table(self.table_name)
        print(f'str_sql_data_types_user_table er {str_sql_data_types_user_table}.')
        df = self.fetch_table(str_sql_data_types_user_table)
        return df

    def drop_table(self):
        print('Er nå inne i drop_table')
        str_drop_user_table = SQL.sql_drop_user_table(self.table_name)
        print(f'str_drop_user_table er {str_drop_user_table}')
        crsr = self.conn.cursor()
        crsr.execute(str_drop_user_table)
        self.conn.commit()
        crsr.close()

    def drop_user_table_if_exists(self):
        # Sjekk om tabellen finnes
        if self.exists_user_table():
            self.drop_table(self.table_name)
    #            

        
        
class Export:
    def __init__(self, conn: oracledb.Connection, clean_output: bool =True):
        self.conn = conn
        self.clean_output = clean_output
    
    def clean_export(self,df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        print('Er nå inne i clean_export')
        tableOps = TableOps(table_name = table_name, conn = self.conn)
        #Innhenter hva som var opprinnelige datatypper 
        db_data_types = tableOps.get_data_types_of_user_table()
        print(f'db_data_types er {db_data_types}')
        sys.exit()
    
    def get_table(self, table_name: str) -> pd.DataFrame:
        tableOps = TableOps(table_name = table_name, conn = self.conn)
        sql_output_table = SQL.generate_select(tableOps.table_name)
        df = tableOps.fetch_table(sql_output_table)
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
        crsr = self.conn.cursor()
        #
        for statement in parsed_sql_query: 
            crsr.execute(statement)
            # Commit the transaction 
            self.conn.commit()
        #
        crsr.close()
        print('Klarte forhåpentlig å gjennomføre hele kjøringen')
        
        #Hvis ønskelig så hentes en tabell ut
        df = None
        if output_table is not None:
            df = self.get_table(output_table)  
            
        print(f'Før avlevering fra run_sql_from_file så er df.__class__ {df.__class__}')      
        return df
    
    
        
    