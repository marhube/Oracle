#******** Start type hinting
from typing import Optional
#******** Slutt type hinting

import os
import sys # For testing
#
import pandas as pd


#******* Start internimport



from Oracle.SQL import SQL_Generator
import Oracle.PrepData as PrepData

#******* Slutt internimport


# The class SQL_Insert does not have its own init.
class SQL_Insert(SQL_Generator):    
    # Lager først en teknisk hjelpefunkjson som først og fremst er ment å håndtere datokolonner (resten blir "as is")
    #Memo til selv: Indekser i Python begynner på 0, mens indekser i Oracle begynner på 1. Må derfor legge til 1.
    # Hivs hardcode_value" er True så settes selve verdien inn i "insert-into"- setningen.
 
  
    #OBSSSSS value kan ha mange ulike datatyper. Må gjøre en uttømmende sjekk av hvilke
    #  Responsen til "conditional_col_transform" kan kun bli dt.datetime hvis enten self.datetime2date er False
    # eller hvis map2db_dtype er satt til å mappe Python datetime til Oracle timestamp.
    def conditional_col_transform(self,
                                  ind: int,
                                  ) -> str:
       
        #Memo til selv: Må ha ekstra fnutter rundt hvis er streng
        print(f'Er inne i conditional_col_transform der ind er {ind}')
        #print('Er inne i conditional_col_transform der self.db_dtypes er {self.db_dtypes}')
        col = self.cols[ind]
        # Memo til selv: Trenger også å vite hvilken kolonne det er snakk om
        col_element = f':{ind+1}'

        if self.db_dtypes[col].upper() == "DATE":       
            col_element = f"TO_DATE({col_element}, '{self.date_format}')"
        elif self.db_dtypes[col].upper() == "TIMESTAMP":       
            col_element = f"TO_TIMESTAMP({col_element}, '{self.timestamp_format}')"
        elif self.db_dtypes[col].upper() in ["NUMBER","INTEGER"]:
            col_element = f"TO_NUMBER({col_element})"
        #
        print(f'Før avlevering fra conditional_col_transform så er col_element {col_element}')        
        return col_element
    #Memo til selv: Har skilt ut første del av "INSERT" i en egen funksjon slik at denne
    # delen også skal kunne brukes av "sql_hardcoded_insert" lenger ned
    #argumentet "include_VALUES" er for å inkludere "VALUES" også med tenke på hardkoding.
    def insert_into_header(self,include_values: bool =False) -> str:
        col_list = ', '.join([f'"{col}"' for col in self.cols])
        collapsed_col_list = f"({col_list})"
        if include_values:
            collapsed_col_list = ' '.join([collapsed_col_list,'VALUES'])
        #
        sql_before_values = f"""INSERT INTO {self.table_name}
            {collapsed_col_list}
        """
        #
        return sql_before_values
    # Memo til selv: Den genererte SQL-koden fra "sql_insert_into" skal typisk kjøres fra Python.
    # "semicolon" er derfor som default her satt til False
    def sql_values_part(self):
        values_inner_part = ', '.join([self.conditional_col_transform(ind) for ind,_ in enumerate(self.cols)])
        values_part = f"VALUES ({values_inner_part})"
        return values_part
   
 
    @staticmethod
    def tabulate(sql_before: str,values_part: str) -> str:
        merged_string = ''.join([sql_before,values_part])
        str_sql_insert_into = '\n\t'.join([line.strip() for line in merged_string.split("\n")])
        return str_sql_insert_into
        
       
    #Memo to self: "sql_insert_into" is now changed so that it no longer (optionnally)
    # adds semicolons to each statement.
    def sql_insert_into(self) -> str:
        print('Er nå inne i sql_insert_into')
        #Lager først første del av "insert into" (før "Values" begynner)
        sql_before_values = self.insert_into_header()
        #Memo til selv: Skal ha poisi
        values_part = self.sql_values_part()
        #Memo til selv: Når har laget en streng med trippelfnutter så blir det "\n" som separator når man joiner denne
        # strengen med en annen streng når man koder "".join(..)
        # Gjør til slutt kodestrengen litt penere med bedre tabulering
        str_sql_insert_into =  SQL_Insert.tabulate(sql_before_values,values_part)    
        return str_sql_insert_into
     
    #    

    def create_or_replace_table(self,df: pd.DataFrame ,table_name: str,**kwargs) -> None:
        pass
    
    
    def preprocess_time_values(self,time_values: pd.Series) -> pd.Series:
        percent_format = replace_time_format(self.date_format)
        time_values = time_values.map(
                    lambda x: x.strftime(percent_format) if not pd.isna(x) else x
                    )
        #
        return time_values
    
    def preprocess_date_values(self,date_values: pd.Series) -> pd.Series:
        date_values = PrepData.datetime2date(date_values)
        if PrepData.get_class_of_series(date_values)  == 'datetime64':
            date_values = datetime2date
            date_values.map(lambda x: x.date())
    
    
    
    def preprocess_obj_values(self,obj_values: pd.Series) -> pd.Series:
        obj_values = obj_values.map(lambda x: None if pd.isna(x) else str(x))
        return obj_values

    #Memo to self: data_series.name extracts colname
    def preprocess_column(self,data_series: pd.Series) ->  pd.Series:
        col = data_series.name
        if self.db_dtypes[col].upper() == "DATE":
            data_series = self.preprocess_time_values(data_series)
        else:
            data_series = self.preprocess_obj_values(data_series)
        #
        return data_series
    
    def preprocess_data(self,df: pd.DataFrame ,copy: bool =True) -> list:             
        print(f'Er nå inne i prepare_data_to_insert der self.db_dtypes er {self.db_dtypes}')
        new_df = None
        if copy:
            new_df = df.copy()
        else:
            new_df = df
        # Integers må derfor gjøres om til "int32" hvis er "int64" Får ellers
        # oracledb.exceptions.NotSupportedError: DPY-3002: Python value of type "int64" is not supported
        
        #
        for col in list(df.columns):
            new_df[col] = self.preprocess_column(new_df[col])
        #
        data_to_insert = [tuple(row) for row in new_df.itertuples(index=False)]
        print('Før avlevering fra  preprocess_data så er data_to_insert')
        print(data_to_insert)        
        return data_to_insert
    #    