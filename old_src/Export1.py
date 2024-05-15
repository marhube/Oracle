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
'

#******* Start internimport
import Oracle.SQL as SQL
#******* Slutt internimport
'

class Execute:
    def __init__(self, crsr: oracledb.Cursor, clean_output: bool =True):
        self.crsr = crsr
        self.clean_output = clean_output
    
    def get_table(self, table_name: str) -> pd.DataFrame:
        sql_output_table = SQL.generate_select(table_name)
        self.crsr.execute(sql_output_table)
        rows = crsr.fetchall()    
    columns = [column[0] for column in crsr.description]
    df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
    if clean_output:
        df = datetime2date(df)
    #
    return df
#