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






class Fetcher:
    def __init__(self, table_name: str, crsr: oracledb.Cursor):
        self.table_name = table_name
        self.crsr = crsr

    def fetch_table(self,table_query) ->  pd.DataFrame:
        self.crsr.execute(table_query)        
        rows = self.crsr.fetchall()
        columns = [column[0] for column in self.crsr.description]
        df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
        return df

    def drop_table(self):
        
    
    
    def exists_user_table(self) -> bool:        
        str_search_for_table  =  SQL.sql_search_user_table(self.table_name)
        print(f'str_search_for_table er {str_search_for_table}')
        df = self.fetch_table(str_search_for_table)
        return df.shape[0] > 0


    def retrieve_data_types_of_user_table(self) -> pd.DataFrame:
        print('Er nå inne i retrieve_data_types_of_user_table')
        str_sql_data_types_user_table = SQL.sql_data_types_user_table(self.able_name)
        print(f'str_sql_data_types_user_table er {str_sql_data_types_user_table}.')
        df = self.fetch_table(str_sql_data_types_user_table)
        return df



    



class Export:
    def __init__(self, crsr: oracledb.Cursor, clean_output: bool =True):
        self.crsr = crsr
        self.clean_output = clean_output
    
    def get_table(self, table_name: str) -> pd.DataFrame:
        sql_output_table = SQL.generate_select(table_name)
        self.crsr.execute(sql_output_table)
        rows = self.crsr.fetchall()
        columns = [column[0] for column in self.crsr.description]
        df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
        if self.clean_output:
            df = PrepData.datetime2date(df)        
        return df
    #
    
    # Kjører Oracle-sql kode
    def run_sql_from_file(self,path_sql: str,sql_query=None,output_table = None) -> Optional[pd.DataFrame]: 
        print('Er nå inne i run_sql_from_file')       
        parsed_sql_query = Parser(path_sql,sql_query=sql_query).parse_sql_code()
        print('parsed_sql_query er')
        print(parsed_sql_query)
        #
        for statement in parsed_sql_query:  
            self.crsr.execute(statement)
            # Commit the transaction if needed
            self.crsr.connection.commit()
        #
        #Hvis ønskelig så hentes en tabell ut
        df = None
        if output_table is not None:
            df = self.get_table(output_table)  
            
        print(f'Før avlevering fra run_sql_from_file så er df.__class__ {df.__class__}')      
        return df
    
    
    