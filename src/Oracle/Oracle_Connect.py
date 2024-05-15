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


def replace_env_variables(s: str) -> str:
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
        connection_string = replace_env_variables(self.get_connection_string())
        engine = create_engine(connection_string)
        return engine
    
    