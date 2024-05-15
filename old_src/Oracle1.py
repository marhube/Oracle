import pandas as pd
import re
import oracledb
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
