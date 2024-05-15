#******** Start type hinting
from typing import Optional
#******** Slutt type hinting

import os
import sys # For testing
#
import pandas as pd
import re
import datetime as dt
import numpy as np


# Memo to self: Converting Oracle dates to Python datetime.date (export table from Oracle to Python )
def datetime2date(df: pd.DataFrame) -> pd.DataFrame:    
    #
    for column in df.columns:
        if get_class_of_series(df[column])  == 'datetime64':
            df[column] = df[column].map(lambda x: x.date()) 
    #
    return df
#





