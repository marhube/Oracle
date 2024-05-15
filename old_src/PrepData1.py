#******** Start type hinting
from typing import Optional
#******** Slutt type hinting

import os
import sys # For testing
#
import pandas as pd
import re
import datetime as dt




def get_class_of_series(series: pd.Series) -> str:
    return series.dtype.type.__name__


def extract_class_name(text: str) -> Optional[str]:
    match = re.search(r"<class '([^']+)'>", text)
    if match is None:
        raise Exception(f"No class is found")
    
    return match.group(1)


#Memo to self: "get_data_type_from_values" only works if the pandas series has length > 0
def get_data_type_from_values(values: pd.Series) -> Optional[str]:
    if len(values.dropna()) == 0:
        raise ValueError("Cannot deduce data type from values when there are no non-missing values")
    
    data_type = list(set([extract_class_name(str(elem.__class__)) for elem in values.dropna()]))    
    return data_type[0]

def get_classes_from_dtypes(dtypes: pd.Series) -> dict:
    class_dict = {}
    for col in dtypes.index:
        class_dict[col] =  extract_class_name(str(dtypes[col].type))
    #
    return class_dict

# Some simple utility functions. Assign "NA" to columns
def robust_get_data_type_from_values(df: pd.DataFrame) -> dict:
    data_types :  {}
    for col in df.columns:
        data_types[col] = None
        values = df[col].dropna()
        if len(values.dropna()) > 0 :
            data_types[col] = get_data_type_from_values(values)
    #
    return data_types
            
            
        
        
    


#Skips columns with no values
def find_date_cols(df: pd.DataFrame) -> list[str]:
    date_cols = []
    for col in df.columns:
        values = df[col].dropna()
        if len(values) > 0 and get_data_type_from_values(values) == "datetime.date":
            date_cols.append(col)
            

#Erstatter her alle datokolonner med "strengedatoer" med forventet format
# "none_for_nan" is part of a workaround to ensure appropriate handling of missing values.
# Note: The standard Python 'int' type does not support missing values, so I would like to use
# "Int64" or 'Int32'. These data types however, are not supported by oracledb. The workaround
# is to convert "Int64" or 'Int32' to string values and uses 'TO_NUMBER()  inside 'execute_many'
# to ensure that these string values are converted to integers inside Oracle (with NULL) for the missing
# values
# er her en del av en "workaround" knyttet til problemet med at
# "standard python int" ikke
def none_for_nan(df: pd.DataFrame,num2str: bool = False) -> pd.DataFrame:   
    print('Er nå inne i none_for_nan')
    # Iterate through each column to check its dtype
    for col in df.columns:
        if get_class_of_series(df[col])  in ['object_','datetime64']:
            # Replace np.nan with None for 'object' dtype or datetime64 columns
            df[col] = df[col].map(lambda x: None if pd.isna(x) else x)
        elif num2str and get_class_of_series(df[col])  in ['float64','int32','int64']:
            df[col] = df[col].map(lambda x: None if pd.isna(x) else str(x))
    return df 
#


#M

def date2datetime(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if get_data_type_from_values(df[col]) == 'datetime.date':
            df[col] = df[col].map(lambda x: dt.datetime.strptime(str(x),'%Y-%m-%d') if not pd.isna(x) else x)
    #
    return df    
#

# Memo to self: Converting Oracle dates to Python datetime.date (export table from Oracle to Python )
def datetime2date(df: pd.DataFrame) -> pd.DataFrame:    
    #
    for column in df.columns:
        if get_class_of_series(df[column])  == 'datetime64':
            df[column] = df[column].map(lambda x: x.date()) 
    #
    return df



