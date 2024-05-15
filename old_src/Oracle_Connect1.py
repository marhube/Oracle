#******** Start type hinting
from typing import Optional
#******** Slutt type hinting

import os
import sys # For testing
import pandas as pd
import datetime as dt
import numpy as np
import re
import oracledb
import sqlalchemy
from sqlalchemy import create_engine

# ***********Start for å lese inn passord + annen db-info
from dotenv import load_dotenv
from decouple import config
# ***Slutt for å lese inn passord + annen db-info

#******* Start internimport
import Oracle
#******* Slutt internimport

class Connect:
    def __init__(
        self,
        data_product: str ='FREG'        
        ) :
        self.data_product = data_product    
    # 
    def get_connection(self) -> oracledb.Connection:
        load_dotenv()        
        hostname = config('_'.join(['ORACLE_HOST',self.data_product]))
        port = config('ORACLE_PORT')
        database_name = config('_'.join(['ORACLE_DB',self.data_product]))
        user = config('ORACLE_USER')
        password =  config('ORACLE_PASSWORD') 
        dsn_tns = oracledb.makedsn(hostname,port,database_name) 
        conn = oracledb.connect(
            user = user,
            password = password,
            dsn=dsn_tns
        )
        return conn
    #
    def get_connection_string(self) -> str:
        dp = self.data_product
        connection_string = f"oracle+oracledb://${{ORACLE_USER}}:${{ORACLE_PASSWORD}}@${{ORACLE_HOST_{dp}}}:${{ORACLE_PORT}}/${{ORACLE_DB_{dp}}}"
        return connection_string

    #Lager  sqlalchemy engine
    def get_engine(self) -> sqlalchemy.engine.base.Engine:
        connection_string = Oracle.replace_env_variables(self.get_connection_string())
        engine = create_engine(connection_string)
        return engine
    
    def create_or_replace_persistent_table(self,df: pd.DataFrame ,table_name: str,**kwargs) -> None:
        # Instansierer først et "SQL_Generator" - objekt
        print('Er nå inne i create_or_replace_persistent_table')
        sql_generator = SQL_Generator(df.dtypes,table_name=table_name,table_type='PERSISTENT',**kwargs)
        #
        print('Er nå tilbake i create_or_replace_persistent_table')
        sql_create = SQL_Create(sql_generator)
        sql_insert = SQL_Insert(sql_generator)
        print('Klarte forhåpentlig å lage både sql_create og sql_insert')
        data_to_insert = sql_insert.prepare_data_to_insert(df)
        # Lager så strengene med sql-kommandoene
        str_create_table = sql_create.sql_create_table()
        str_insert_into = sql_insert.sql_insert_into()
        print('str_insert_into er ')
        print(str_insert_into)
        print('data_to_insert er')
        print(data_to_insert)
        #
        cx_conn = self.get_connection()
        crsr = cx_conn.cursor()
        if exists_table(sql_generator.table_name,crsr):
            crsr.execute(f"DROP TABLE {sql_generator.table_name}")
        #
        crsr.execute(str_create_table)
        cx_conn.commit()
        crsr.executemany(str_insert_into,data_to_insert)
        cx_conn.commit()
        cx_conn.close()

    
    
    # Kjører Oracle-sql kode
    def run_sql_from_file(self,
                          path_sql: str,                          
                          sql_query=None,
                          output_table = None,
                          clean_output = True) -> Optional[pd.DataFrame]  :
        
        parsed_sql_query = SQL_Parser(path_sql,sql_query=sql_query).parse_sql_code()
        print('parsed_sql_query er')
        print(parsed_sql_query)
        #
        conn = self.get_connection()
        crsr = conn.cursor()
        for statement in parsed_sql_query:  
            crsr.execute(statement)
            # Commit the transaction if needed
            crsr.connection.commit()
        #
        #Hvis ønskelig så hentes en tabell ut
        df = None
        if output_table is not None:
            df = get_table(output_table,crsr,clean_output=clean_output)        
        # Kobler fra databasen
        crsr.close()
        conn.close()
        #
        return df
#    
    