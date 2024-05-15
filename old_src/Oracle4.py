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

# Utility function NB!!!!!!!! Disse blir pr.nå ikke testet av enhetstesteren
def generate_select(table_name: str) -> str:
    select_str = """SELECT *  """ + f"FROM {table_name}"
    return select_str
# Legger nå til en "rensefunksjon" som gjør  "SQL"-date kun blir "date" og ikke "datetime"
def clean_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    for enum,column in enumerate(list(df.columns)):
        if str(df.dtypes[enum]) in ['datetime64','datetime64[ns]']:
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
        df = clean_date_columns(df)
    #
    return df
#
def run_sql_from_file(path_sql: str,oracle_connect,sql_query=None,output_table = None,clean_output = True):
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


#sql_gen skal være en instans av "SQL_Generator"
def create_or_replace_persistent_table(df,table_name,oracle_connect,**kwargs):
    # Instansierer først et "SQL_Generator" - objekt
    sql_gen_commit = SQL_Generator(
        df.dtypes,
        table_name=table_name,
        table_type='PERSISTENT',
        **kwargs
    )
    #
    sql_create_commit = SQL_Create(sql_gen_commit)
    sql_insert_commit = SQL_Insert(sql_gen_commit)
    data_to_insert = sql_insert_commit.prepare_data_to_insert(df)
    # Lager så strengene med sql-kommandoene
    str_create_table_commit = sql_create_commit.sql_create_table()
    str_insert_into_commit = sql_insert_commit.sql_insert_into()
    #
    cx_conn = oracle_connect.get_connection()
    crsr = cx_conn.cursor()
    if exists_table(sql_gen_commit.table_name,crsr):
        crsr.execute(f"DROP TABLE {sql_gen_commit.table_name}")
    #
    crsr.execute(str_create_table_commit)
    cx_conn.commit()
    crsr.executemany(str_insert_into_commit,data_to_insert)
    cx_conn.commit()
    cx_conn.close()
#



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
 
 # Lager så en klasse "SQL_Generator"
class SQL_Generator:
    #Variabelen "datetime2date" sier om datatypen "datetime" i pandas skal settes som
    #"DATE" eller  som timestamp.  Motivasjonen bak å sette datetime til DATE er at 
    #tidfested informajon i Folkeregisteret kun angir dato.
    #Er nå valgfritt om man vil ha med ";" på slutten eller ikke. For å kjøre i Python (oracledb) 
    # kan ";" ikke være med.
    # MEmo til selv: "semicolon" brukes av både "SQL_Create" og "SQL"
    def __init__(
        self,dtypes,table_name,table_type='PRIVATE',datetime2date=True,varchar_size=255,
        dateformat = "YYYY-MM-DD"
        ):
        #Sjekker først at 
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
        self.orig_dtypes =  [str(dtype) for dtype in dtypes]
        self.dtypes = self.map2db_dtype() 
        #Memo til selv: Datoformatet i databasen er satt opp til å være "DD.MM.YY"
        self.date_format = dateformat
        #       
    #Funksjon for å omgjøre "potensielt problematiske navn på tabell". Fjerner punktum og mellomrom
    #
    @staticmethod
    def clean_name(name):
        patterns = ["\.+","\s+"]
        for pattern in patterns:
            name = re.sub(pattern=pattern,repl='_',string=name)
        #Fjerner til slutt eventuelle "etterfølgende underscores"
        name = re.sub('_+',repl="_",string=name)
        return name
    #Memo til selv: Kolonnenavn kan "omsluttes" med dobbeltfnutter slik at de kan inneholde omtrent hva
    # som helst av tegn, men det samme er ikke tilfelle for tabellnavn.
    def clean_table_name(self):
        table_name = SQL_Generator.clean_name(self.orig_table_name)
        #Navn på "PRIVATE"-tabeller må i Oracle av en eller annen grunn starte med "ORA$PPT_"
        if self.table_type.upper() == "PRIVATE" and not table_name.upper().startswith(self.temp_prefix):
            table_name = self.temp_prefix + table_name
        #
        return table_name
    #
    def clean_colnames(self):
        #"padder" kolonnenavn med fnutter for at de skal bli robuste
        new_colnames = [f'"{col}"' for col in self.orig_cols]
        #Hvis kolonnenavn
        new_colnames = [re.sub('"+',repl='"',string=col) for col in new_colnames]
        return new_colnames
    #
    def map2db_dtype(self):
        #Hvis ønskelig (som forklart i init over) så mappes datetime til DATE
        oracle_datetime = "TIMESTAMP"
        if self.datetime2date:
            oracle_datetime = 'DATE'
        #
        dtype_mapping = {
            'int32': 'INTEGER',
            'int64': 'INTEGER',
            'float64': 'NUMBER',
            'object': f'VARCHAR2({self.varchar_size})',
            'datetime64': oracle_datetime,
            'datetime64[ns]': oracle_datetime,
            'datetime64[us]': oracle_datetime # Må dekke tilfellet med millisekunder
        }
        #
        oracle_dtypes = [dtype_mapping[dtype] for dtype in  self.orig_dtypes]
        return oracle_dtypes
    #
#
 
 
 
 
    
    #Lager så en klasse "SQL_Create"
class SQL_Create:
    #
    def __init__(self, sql_generator,preserve_definition = True,preserve_rows = False,semicolon = False):
        self.sql_generator = sql_generator
        self.preserve_definition = preserve_definition
        self.preserve_rows = preserve_rows
        self.semicolon = semicolon
    #
    def sql_create_table(self):
        # Memo til selv: Må ha med "innskutt tabulator (\t) for at det skal se pent ut"
        col_coltype_list = ',\n\t'.join(
            [' '.join([f'"{self.sql_generator.cols[ind]}"',self.sql_generator.dtypes[ind]]) for ind in range(len(self.sql_generator.cols))])    
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
    def __init__(self,sql_generator):
        self.sql_generator = sql_generator
    #       
    # Lager først en teknisk hjelpefunkjson som først og fremst er ment å håndtere datokolonner (resten blir "as is")
    #Memo til selv: Indekser i Python begynner på 0, mens indekser i Oracle begynner på 1. Må derfor legge til 1.
    # Hivs hardcode_value" er True så settes selve verdien inn i "insert-into"- setningen.
    @staticmethod
    def replace_date_format(input_str):
        # Replace 'YYYY' with '%Y', 'MM' with '%m', and 'dd' with '%d'
        output_str = input_str.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
        return output_str
    #
    def conditional_col_transform(self,ind,value=None,hardcode_value=False):
        col_element = f':{ind+1}'
        if hardcode_value:          
            #Hvis "datetime2date" er satt så skal kun datodelen av datetime-verdier settes
            # (dette er tenkt for "hardkodet insert" fra sql_hardcoded_insert)
            #Memo til selv: Må ha ekstra fnutter rundt hvis er streng
            if self.sql_generator.dtypes[ind].upper() == "DATE" and self.sql_generator.datetime2date: 
                # Memo til selv; Gjør om datoformatet til "%-form"
                string_date_value = value.strftime(SQL_Insert.replace_date_format(self.sql_generator.date_format))
                value = f"'{string_date_value}'"
            elif isinstance(value,str):
                value = f"'{value}'"            
            #Må også håndtere manglende verdier spesielt
            elif value is None or math.isnan(value):
                value = "NULL" 
            #
            col_element = value
        #Memo til selv: Må ha ekstra fnutter rundt hvis er streng
        if self.sql_generator.dtypes[ind].upper() == "DATE":       
            col_element = f"TO_DATE({col_element}, '{self.sql_generator.date_format}')"
         #        
        return col_element
    #Memo til selv: Har skilt ut første del av "INSERT" i en egen funksjon slik at denne
    # delen også skal kunne brukes av "sql_hardcoded_insert" lenger ned
    #argumentet "include_VALUES" er for å inkludere "VALUES" også med tenke på hardkoding.
    def insert_into_header(self,include_values=False):
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
    def sql_insert_into(self,semicolon = False):
        #Lager først første del av "insert into" (før "Values" begynner)
        sql_before_values = self.insert_into_header()
        values_inner_part = ', '.join([self.conditional_col_transform(ind) for ind in range(len(self.sql_generator.cols))])
        values_part = f"VALUES ({values_inner_part})"
        #Memo til selv: Når har laget en streng med trippelfnutter så blir det "\n" som separator når man joiner denne
        # strengen med en annen streng når man koder "".join(..)
        sql_insert_into = ''.join([sql_before_values,values_part])
        if semicolon:
            sql_insert_into = sql_insert_into + ";"
        # Gjør til slutt kodestrengen litt penere med bedre tabulering
        sql_insert_into = '\n\t'.join([line.strip() for line in sql_insert_into.split("\n")])
        return sql_insert_into
    #Erstatter her alle datokolonner med "strengedatoer" med forventet format
    @staticmethod
    def none_for_nan(df):   
        # Iterate through each column to check its dtype
        for col in df.columns:
            if df[col].dtype == 'object' or np.issubdtype(df[col].dtype, np.datetime64):
                # Replace np.nan with None for 'object' dtype or datetime64 columns
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
        #
        return df    
    #    
    def prepare_data_to_insert(self,df,copy=True):
        new_df = None
        if copy:
            new_df = df.copy()
        else:
            new_df = df
        #
        for enum,col in enumerate(list(df.columns)):
            if self.sql_generator.dtypes[enum].upper() == "DATE" and self.sql_generator.datetime2date:
                date_percent_format = SQL_Insert.replace_date_format(self.sql_generator.date_format)
                new_df[col] = new_df[col].map(lambda x: x.strftime(date_percent_format))
        #
        new_df = SQL_Insert.none_for_nan(new_df)
        data_to_insert = [tuple(row) for row in new_df.itertuples(index=False)]        
        return data_to_insert
        #Må bruke if self.sql_generator.dtypes[ind].upper() == "DATE" and self.sql_generator.datetime2date
    
    # Memo til selv: Den genererte SQL-koden fra sql_hardcoded_insert skal typisk kjøres inne i SQL Developer.
    # "semicolon" er derfor som default her satt til True
    def sql_hardcoded_insert(self,df,semicolon=True):
        # Gjør først om dataene til en liste
        listed_data = df.values.tolist()
        hardcoded_insert = self.insert_into_header(include_values=True).replace('\n'," ")
        #Renser bort "flere mellomrom på rad."
        hardcoded_insert = re.sub(pattern = "\s+",repl=" ",string=hardcoded_insert) 
        # Lager så strenger der hver streng inneholder "insert" 
        # for verdier til en record
        list_insertions = []
        for datapoint in listed_data:
            # Trekk ut enkeltverdier fra en enkelt rad i datarammen
            #Memo til selv: Må gjøre "str" på output fra "conditional_col_transform" og ikke på input "value"
            # 
            str_collapsed_values = ', '.join([str(self.conditional_col_transform(i,value,hardcode_value=True)) 
                                            for i,value in enumerate(datapoint)])
            #Legger til neste rad
            next_insert = "\n".join([hardcoded_insert,f"({str_collapsed_values})"])           
            if semicolon:
                next_insert = next_insert + ";"
            #
            list_insertions.append(next_insert)
        #
        all_insertions = "\n".join(list_insertions)
        return all_insertions
    #
    