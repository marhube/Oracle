#******** Start type hinting
from typing import Optional
#******** Slutt type hinting

import os
import sys # For testing
import pandas as pd
import re
import oracledb
import sqlalchemy
from sqlalchemy import create_engine
from decouple import config
import datetime as dt
import numpy as np
#
import Utility
# ***********Start for å lese inn passord + annen db-info
from dotenv import load_dotenv
from decouple import config
# ***Slutt for å lese inn passord + annen db-info
#

# Leser først inn kode for å kunne koble på databasen

#



# Utility function 
def generate_select(table_name: str) -> str:
    select_str = """SELECT *  """ + f"FROM {table_name}"
    return select_str
# Legger nå til en "rensefunksjon" som gjør  "SQL"-date kun blir "date" og ikke "datetime"

# Funksjon for å avgjøre om en enkelt kolonne i en pandas dataframe er en datetime
#Memo til selv: Nyttig info: 
# Utility function to obtain class of a pandas series and from dtypes
def get_class_of_series(series: pd.Series) -> str:
    return series.dtype.type.__name__
#


def extract_class_name(text: str) -> str:
    match = re.search(r"<class '([^']+)'>", text)
    if match:
        return match.group(1)
    else:
        return None


#Memo to self: "get_data_type_from_values" only works if the pandas series has length > 0
def get_data_type_from_values(values: pd.Series) -> str:
    print('Er nå inne i map2db_from_values')
    if len(values) == 0:
        raise ValueError("Cannot deduce data type from values when there are no values")
    
    data_type = list(set([extract_class_name(str(elem.__class__)) for elem in values.dropna()]))    
    return data_type[0]
#

def get_classes_from_dtypes(dtypes: pd.Series) -> str:
    print('Er nå inne i get_classes_from_dtypes')
    class_dict = {}
    for col in dtypes.index:
        class_dict[col] =  extract_class_name(str(dtypes[col].type))
    #
    return class_dict
#

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


#Memo to self: In the "FREG-database" there are only dates, not timestamps.


def date2datetime(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if get_data_type_from_values(df[col]) == 'datetime.date':
            df[col] = df[col].map(lambda x: dt.datetime.strptime(str(x),'%Y-%m-%d'))
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
#


def get_table(table_name: str, crsr: oracledb.Cursor, clean_output: bool =True) -> pd.DataFrame: 
    sql_output_table = generate_select(table_name)
    crsr.execute(sql_output_table)
    rows = crsr.fetchall()    
    columns = [column[0] for column in crsr.description]
    df = pd.DataFrame((tuple(row) for row in rows), columns=columns)
    if clean_output:
        df = datetime2date(df)
    #
    return df
#

def exists_table(table_name: str ,crsr: oracledb.Cursor) -> bool:
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
        connection_string = Utility.replace_env_variables(self.get_connection_string())
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
    
 
# Lager så en klasse "SQL_Generator"
class SQL_Generator:
    #Variabelen "datetime2date" sier om datatypen "datetime" i pandas skal settes som
    #"DATE" eller  som timestamp.  Motivasjonen bak å sette datetime til DATE er at 
    #tidfested informajon i Folkeregisteret kun angir dato.
    #Er nå valgfritt om man vil ha med ";" på slutten eller ikke. For å kjøre i Python (oracledb) 
    # kan ";" ikke være med.
    # MEmo til selv: "semicolon" brukes av både "SQL_Create" og "SQL"
    def __init__(
        self,
        dtypes: pd.core.series.Series,
        table_name: str,
        table_type: str ='PRIVATE',
        datetime2date: bool = False,
        varchar_size: int = 255,
        dateformat: str = "YYYY-MM-DD"
        ):
        #Sjekker først at 
        print('Er nå inne i init-funksjonen til klassen SQL_Generator')
        permitted_data_types = ['PRIVATE','GLOBAL','PERSISTENT'] 
        if table_type.upper() not in permitted_data_types:
            raise Exception(f"table_type has to be one of {permitted_data_types}.")
        #Navn på "PRIVATE"-tabeller må i Oracle av en eller annen grunn starte med "ORA$PPT_"
        self.temp_prefix = "ORA$PTT_"
        self.table_type=table_type
        self.varchar_size = varchar_size
        self.datetime2date = datetime2date
        self.orig_table_name = table_name
        self.table_name = self.clean_table_name()
        self.cols = list(dtypes.index)
        self.str_dtypes =  [str(dtype) for dtype in dtypes]
        self.dtypes = get_classes_from_dtypes(dtypes)
        print(f'self.dtypes er {self.dtypes}')
        # Merk: Mappingen er nå en dictionary
        self.db_dtypes =  self.map2db_dtype() 
        #Memo til selv: Datoformatet i databasen er satt opp til å være "DD.MM.YY"
        self.date_format = dateformat
        #       
    #Funksjon for å omgjøre "potensielt problematiske navn på tabell". Fjerner punktum og mellomrom
    #
    @staticmethod
    def clean_name(name: str) -> str:
        patterns = ["\.+","\s+"]
        for pattern in patterns:
            name = re.sub(pattern=pattern,repl='_',string=name)
        #Fjerner til slutt eventuelle "etterfølgende underscores"
        name = re.sub('_+',repl="_",string=name)
        return name
    #Memo til selv: Kolonnenavn kan "omsluttes" med dobbeltfnutter slik at de kan inneholde omtrent hva
    # som helst av tegn, men det samme er ikke tilfelle for tabellnavn.
    def clean_table_name(self) -> str:
        table_name = SQL_Generator.clean_name(self.orig_table_name)
        #Navn på "PRIVATE"-tabeller må i Oracle av en eller annen grunn starte med "ORA$PPT_"
        if self.table_type.upper() == "PRIVATE" and not table_name.upper().startswith(self.temp_prefix):
            table_name = self.temp_prefix + table_name
        #
        return table_name
    #
    def map2db_dtype(self) -> dict:
        #Hvis ønskelig (som forklart i init over) så mappes datetime til DATE
        oracle_datetime = "TIMESTAMP"
        if self.datetime2date:
            oracle_datetime = 'DATE'
        #
        dtype_mapping = {
            'numpy.int32': 'INTEGER',
            'numpy.int64': 'INTEGER',
            'numpy.float64': 'NUMBER',
            'numpy.object_': f'VARCHAR2({self.varchar_size})',
            'numpy.datetime64': oracle_datetime
        }
        #
        oracle_dtypes = {}
        
        for key,value in self.dtypes.items():
            oracle_dtypes[key] = dtype_mapping[value]
        return oracle_dtypes
    #
#
 
 
 
 
    
#Lager så en klasse "SQL_Create"
class SQL_Create:
    #
    def __init__(self, 
                 sql_generator: SQL_Generator,
                 preserve_definition: bool = True,
                 preserve_rows: bool = False,
                 semicolon: bool = False):
        self.sql_generator = sql_generator
        self.preserve_definition = preserve_definition
        self.preserve_rows = preserve_rows
        self.semicolon = semicolon
    #
    def sql_create_table(self) -> str:
        print('Er nå inne i sql_create_table der self.sql_generator.dtypes er')
        print(self.sql_generator.dtypes)
        print('og self.sql_generator.db_dtypes er')
        print(self.sql_generator.db_dtypes)
        # Memo til selv: Må ha med "innskutt tabulator (\t) for at det skal se pent ut"
        col_coltype_list = ',\n\t'.join(
            [' '.join([f'"{col}"',self.sql_generator.db_dtypes[col]]) for col in self.sql_generator.cols])
                    
        #       
        create_table_subparts = ['CREATE',"TABLE",self.sql_generator.table_name]
        table_post_option = ""
        #Memo til selv: Må ha et ekstra mellomrom på slutten for at "TEMPORARY" ikke skal "kollidere" med "TABLE".
        #Memo til selv: FRa https://stackoverflow.com/questions/51272128/trying-to-create-a-temp-table-in-oracle for forklaring
        # om "OM COMMIT PRESERVE DEFINITION"
        #Memo til selv: For å kunne fungere som en "commit" i Python må man ikke ha med semikolon.
        # Memo til selv: For "GLOBAL TEMPORARY TABLE" så er det flere valgmuligheter enn bare "ON COMMIT PRESERVE DEFINITION",
        if self.sql_generator.table_type.upper() in ["PRIVATE","GLOBAL"]:
            create_table_subparts.insert(1,' '.join([self.sql_generator.table_type.upper(),"TEMPORARY"]))
            table_post_option = "ON COMMIT PRESERVE DEFINITION"
            if not self.preserve_definition:
                table_post_option = "ON COMMIT DROP DEFINITION"
            #
            if self.sql_generator.table_type.upper() == "GLOBAL" and self.preserve_rows:
                table_post_option = "ON COMMIT PRESERVE ROWS"
        #
        # Memo til selv: Må settes opp slik for at det skal se "riktig" ut tabulatormessig.
        create_table_part = ' '.join(create_table_subparts)
        statement = f"""{create_table_part} ( 
        {col_coltype_list}
        ) {table_post_option}
        """
        if self.semicolon:
            statement = statement + ";"
        return statement
    #
#

#Lager så en klasse "SQL_Insert"
class SQL_Insert:
    def __init__(self,sql_generator: SQL_Generator):
        self.sql_generator = sql_generator
    #       
    # Lager først en teknisk hjelpefunkjson som først og fremst er ment å håndtere datokolonner (resten blir "as is")
    #Memo til selv: Indekser i Python begynner på 0, mens indekser i Oracle begynner på 1. Må derfor legge til 1.
    # Hivs hardcode_value" er True så settes selve verdien inn i "insert-into"- setningen.
    @staticmethod
    def replace_date_format(input_str: str) -> str:
        # Replace 'YYYY' with '%Y', 'MM' with '%m', and 'dd' with '%d'
        output_str = input_str.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
        return output_str
    
    #OBSSSSS value kan ha mange ulike datatyper. Må gjøre en uttømmende sjekk av hvilke
    #  Responsen til "conditional_col_transform" kan kun bli dt.datetime hvis enten self.sql_generator.datetime2date er False
    # eller hvis map2db_dtype er satt til å mappe Python datetime til Oracle timestamp.
    def conditional_col_transform(self,
                                  ind: int,
                                  ) -> str:
       
        #Memo til selv: Må ha ekstra fnutter rundt hvis er streng
        print(f'Er inne i conditional_col_transform der ind er {ind}')
        #print('Er inne i conditional_col_transform der self.sql_generator.db_dtypes er {self.sql_generator.db_dtypes}')
        col = self.sql_generator.cols[ind]
        # Memo til selv: Trenger også å vite hvilken kolonne det er snakk om
        col_element = f':{ind+1}'

        if self.sql_generator.db_dtypes[col].upper() == "DATE":       
            col_element = f"TO_DATE({col_element}, '{self.sql_generator.date_format}')"
        elif self.sql_generator.db_dtypes[col].upper() in ["NUMBER","INTEGER"]:
            col_element = f"TO_NUMBER({col_element})"
        #
        print(f'Før avlevering fra conditional_col_transform så er col_element {col_element}')        
        return col_element
    #Memo til selv: Har skilt ut første del av "INSERT" i en egen funksjon slik at denne
    # delen også skal kunne brukes av "sql_hardcoded_insert" lenger ned
    #argumentet "include_VALUES" er for å inkludere "VALUES" også med tenke på hardkoding.
    def insert_into_header(self,include_values: bool =False) -> str:
        col_list = ', '.join([f'"{col}"' for col in self.sql_generator.cols])
        collapsed_col_list = f"({col_list})"
        if include_values:
            collapsed_col_list = ' '.join([collapsed_col_list,'VALUES'])
        #
        sql_before_values = f"""INSERT INTO {self.sql_generator.table_name}
            {collapsed_col_list}
        """
        #
        return sql_before_values
    # Memo til selv: Den genererte SQL-koden fra "sql_insert_into" skal typisk kjøres fra Python.
    # "semicolon" er derfor som default her satt til False
    def sql_insert_into(self,semicolon: bool = False) -> str:
        print('Er nå inne i sql_insert_into')
        #Lager først første del av "insert into" (før "Values" begynner)
        sql_before_values = self.insert_into_header()
        #Memo til selv: Skal ha poisi
        values_inner_part = ', '.join([self.conditional_col_transform(ind) for ind,_ in enumerate(self.sql_generator.cols)])
        values_part = f"VALUES ({values_inner_part})"
        print('values_part er')
        print(values_part)
        #Memo til selv: Når har laget en streng med trippelfnutter så blir det "\n" som separator når man joiner denne
        # strengen med en annen streng når man koder "".join(..)
        sql_insert_into = ''.join([sql_before_values,values_part])
        if semicolon:
            sql_insert_into = sql_insert_into + ";"
        # Gjør til slutt kodestrengen litt penere med bedre tabulering
        sql_insert_into = '\n\t'.join([line.strip() for line in sql_insert_into.split("\n")])
        return sql_insert_into
     
    #    
        
    def prepare_data_to_insert(self,df: pd.DataFrame ,copy: bool =True) -> list:             
        print(f'Er nå inne i prepare_data_to_insert der self.sql_generator.db_dtypes er {self.sql_generator.db_dtypes}')
        new_df = None
        if copy:
            new_df = df.copy()
        else:
            new_df = df
        # Integers må derfor gjøres om til "int32" hvis er "int64" Får ellers
        # oracledb.exceptions.NotSupportedError: DPY-3002: Python value of type "int64" is not supported
        
        #
        for col in list(df.columns): 
            if self.sql_generator.db_dtypes[col].upper() == "DATE" and self.sql_generator.datetime2date:
                date_percent_format = SQL_Insert.replace_date_format(self.sql_generator.date_format)
                #Tar nå hensyn til at strftime ikke kan ha "missing" som input
                new_df[col] = new_df[col].map(
                    lambda x: x.strftime(date_percent_format) if not pd.isna(x) else x
                    )
        #
        new_df = none_for_nan(new_df,num2str=True)
        data_to_insert = [tuple(row) for row in new_df.itertuples(index=False)]
        print('Før avlevering fra  prepare_data_to_insert så er data_to_insert')
        print(data_to_insert)        
        return data_to_insert
    #    
    
    
    
class SQL_Parser:
    def __init__(
        self,
        sql_file_path: Optional[str] = None,
        sql_query: Optional[str] = None    
        ):
        self.sql_file_path = sql_file_path
        # Hvis ønskelig les sql-koden inn fra fil
        if self.sql_file_path is not None:            
            self.sql_orig_query = self.get_sql_code()
        else:
            self.sql_orig_query = sql_query
        #
        self.sql_query = self.remove_comments_and_empty_lines(self.sql_orig_query)
    #
    def get_sql_code(self) -> str:
        sql_code = None
        # Get lines of code
        with open(self.sql_file_path, "r", encoding="latin-1") as file:
            sql_code = file.read()
        return sql_code
    #
    @staticmethod # Denne funksjonen er nyttig å bruke i andre sammenhenger, derfor statisk
    def remove_comments_and_empty_lines(sql_code) -> str:
        # Regular expression to match comments starting with "--"
        comment_pattern = re.compile(r'--[^\n]*')
        # Split the code into lines
        lines = sql_code.split('\n')        
        cleaned_lines = []
        #
        for line in lines:            
            line = re.sub(comment_pattern, '', line) # Remove comments from the line
            line = line.strip() # Remove leading and trailing spaces from the line
            # Add the cleaned line to the list if it's not empty
            if line:
                cleaned_lines.append(line)
        # Join the cleaned lines with line breaks
        cleaned_code = '\n'.join(cleaned_lines)
        return cleaned_code
    #Memo til selv: Klassen StatementHandler tar nå i stor grad hånd om det som
    # Pylance i Visual Studio Studio Code klarer ikke å "se " klasser definer i filen
    class StatementHandler:
        def __init__(self,statement: str = "",current_block: list = [],execution_order: list = []):
            #current_block: - Temporary list for the current control structure block
            # execution_order<: List to hold statements in execution order
            self.statement = statement.strip()  # Remove leading/trailing whitespaces
            self.current_block = current_block
            self.execution_order = execution_order
        #
        def update_statement(self,statement: str) -> 'StatementHandler':
            self.statement = SQL_Parser.remove_comments_and_empty_lines(statement)            
            return self
        # 
        def is_control_structure_start(self) -> bool:
            # Check if the statement starts with "DECLARE" or "BEGIN" (case-insensitive)
            lower_case_statement = self.statement.lower()
            #
            starts_control_structure = (
                lower_case_statement.startswith('declare') or 
                lower_case_statement.startswith('begin')
            )            
            return starts_control_structure
        #
        def handle_control_structure(self) -> 'StatementHandler' : 
            if self.statement == '/':
                # If "/" is encountered, it marks the end of a control structure block
                # Join the statements within the block and add to the execution order
                self.execution_order.append(' '.join(self.current_block))
                self.current_block.clear()  # Clear the current block for the next control structure
            else:
                # Add the statement to the current control structure block, retaining the semicolon
                self.current_block.append(self.statement + ";")
            #
            return self
        #
        def handle_individual_statement(self) -> 'StatementHandler':
            # Add individual statements to the execution order, if not empty
            if self.statement:                
                self.execution_order.append(self.statement)
            #
            return self                                  
    #    
    def split_on_control_structures(self) -> list:
        #Initialiserer en "tom StatementHandler" utenfor loopen
        handler = self.StatementHandler()
        inside_control_structure = False
        # Split the code into statements using semicolons
        for statement in self.sql_query.split(';'):            
            handler.update_statement(statement)
            # Check if the statement marks the start of a control structure
            if handler.is_control_structure_start():
                inside_control_structure = True
            #
            if inside_control_structure:
                # Handle control structure statements
                handler.handle_control_structure()
            else:
                # Handle individual statements
                handler.handle_individual_statement()
        #
        return handler.execution_order
    #Memo til selv: remove_comments_and_empty_lines er nå statisk
    def parse_sql_code(self) -> list:
        sql_statements = self.split_on_control_structures()
        return sql_statements
    #