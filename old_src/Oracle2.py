import os
import sys
import pandas as pd
import re
import oracledb
from sqlalchemy import create_engine
from decouple import config
import datetime as dt
import numpy as np
#
# ***********Start for å lese inn passord + annen db-info
from functools import partial # For partial functions
from dotenv import load_dotenv
from decouple import config
# ***Slutt for å lese inn passord + annen db-info
#
# Leser først inn kode for å kunne koble på databasen



def replace_env_variables(s):
    # This function will find all occurrences of ${VAR_NAME} in the string
    # and replace them with the value of the environment variable VAR_NAME.
    # Define a pattern to match ${ANYTHING_HERE}
    pattern = re.compile(r'\$\{(.+?)\}')
    # Replace each found pattern with the corresponding environment variable
    def replace(match):
        var_name = match.group(1)  # Extract variable name
        return os.environ.get(var_name, '')  # Replace with env variable value
    #
    return pattern.sub(replace, s)

# Utility function NB!!!!!!!! Disse blir pr.nå ikke testet av enhetstesteren
def generate_select(table_name):
    select_str = """SELECT *  """ + f"FROM {table_name}"
    return select_str
# Legger nå til en "rensefunksjon" som gjør  "SQL"-date kun blir "date" og ikke "datetime"
def clean_date_columns(df):
    for enum,column in enumerate(list(df.columns)):
        if str(df.dtypes[enum]) in ['datetime64','datetime64[ns]']:
            df[column] = df[column].map(lambda x: x.date()) 
    #
    return df
#
def get_table(table_name,crsr,clean_output=True):
    sql_output_table = generate_select(table_name)
    crsr.execute(sql_output_table)
    rows = crsr.fetchall()    
    columns = [column[0] for column in crsr.description]
    df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
    if clean_output:
        df = clean_date_columns(df)
    #
    return df
#
def run_sql_from_file(path_sql,oracle_connect,sql_query=None,output_table = None,clean_output = True):
    parsed_sql_query = SQL_Parser(path_sql,sql_query=sql_query).parse_sql_code()
    print('parsed_sql_query er')
    print(parsed_sql_query)
    #
    conn = oracle_connect.get_connection()
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

def exists_table(table_name,crsr):
    str_search_for_table  = f"SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME = '{table_name}'"
    print(f'str_search_for_table er {str_search_for_table}')
    crsr.execute(str_search_for_table)
    rows = crsr.fetchall()
    columns = [column[0] for column in crsr.description]
    df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
    return df.shape[0] > 0


# Definerer en klasse for å håndtere pålogging
# Henter nå brukernavn og passord fra en ".env-file"

class Oracle_Connect:
    def __init__(
        self,
        data_product='FREG'        
        ):
        self.data_product = data_product    
    # 
    def get_connection(self):
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
    def get_connection_string(self):
        dp = self.data_product
        connection_string = f"oracle+oracledb://${{ORACLE_USER}}:${{ORACLE_PASSWORD}}@${{ORACLE_HOST_{dp}}}:${{ORACLE_PORT}}/${{ORACLE_DB_{dp}}}"
        return connection_string

    #Lager  sqlalchemy engine
    def get_engine(self):
        connection_string = replace_env_variables(self.get_connection_string())
        engine = create_engine(connection_string)
        return engine
 
    
    