#********** For testing
import os
import sys
#*******
import re
import pandas as pd
#**********
from typing import Optional
#********

#******* Start internimport
import Oracle.PrepData as PrepData
#******* Slutt internimport

def remove_comments_and_empty_lines(sql_code: str) -> str:
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


def generate_select(table_name: str) -> str:
    select_str = """SELECT *  """ + f"FROM {table_name}"
    return select_str

#Utility function to check if a table is a private temporary table
def is_private_temp_table(table_name: str) -> bool:
    is_private = table_name.upper().startswith("ORA$PTT_")
    return is_private

def find_tablespace(table_name: str) -> str:
    tablespace = "USER_TABLES"
    if is_private_temp_table(table_name):
        tablespace = "USER_PRIVATE_TEMP_TAB_COLUMNS"    
    #
    
    return tablespace

def sql_search_user_table(table_name: str) -> str:
    tablespace = find_tablespace(table_name)    
    
    str_search_for_table  = f"""
    SELECT TABLE_NAME FROM {tablespace} 
    WHERE TABLE_NAME LIKE '%{table_name}%'
    """
    return remove_comments_and_empty_lines(str_search_for_table)

def sql_retrieve_exact_table_name(table_name: str) -> str:
    tablespace = find_tablespace(table_name)
    str_retrieve_exact_table_name = f'''
    SELECT table_name FROM {tablespace} WHERE table_name LIKE '%{table_name}%'
    '''
    return remove_comments_and_empty_lines(str_retrieve_exact_table_name)

# Memo to self: sql_data_types_user_table only works for persistent tables
def sql_data_types_user_table(table_name: str) -> str:
    str_retrieve_exact_table_name = sql_retrieve_exact_table_name(table_name)
    #
    str_data_types  = f"""
    SELECT COLUMN_NAME,DATA_TYPE,TABLE_NAME
    FROM user_tab_columns
    WHERE table_name = ({str_retrieve_exact_table_name})
    """    
    return remove_comments_and_empty_lines(str_data_types)

#

def sql_drop_user_table(table_name: str) -> str:
    str_drop_user_table = f'DROP TABLE "{table_name}"'
    return str_drop_user_table
        





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
        self.dtypes = PrepData.get_classes_from_dtypes(dtypes)
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


class Parser:
    def __init__(
        self,
        sql_file_path: Optional[str] = None,
        sql_query: Optional[str] = None    
        ):
        #If sql_file_path and sql_query are both None then there is nothing to parse.
        # As for now I handle this by throwing an exception
        
        if sql_file_path is None and sql_query is None:
            raise Exception("sql_file_path and sql_query cannot both be None")          
         
        if sql_file_path is not None:
            self.sql_file_path = sql_file_path                       
            self.sql_orig_query = self.get_sql_code()
        elif sql_query is not None: 
            self.sql_orig_query = sql_query
        #
        self.sql_query = remove_comments_and_empty_lines(self.sql_orig_query)
    #     
    
    def get_sql_code(self) -> str:
        sql_code = ""
        # Get lines of code
        with open(self.sql_file_path, "r", encoding="latin-1") as file:
            sql_code = file.read()
        return sql_code
    #

    #Memo til selv: Klassen StatementHandler tar nå i stor grad hånd om det som
    # Pylance i Visual Studio Studio Code klarer ikke å "se " klasser definer i filen
    class StatementHandler:
        def __init__(self):
            #current_block: - Temporary list for the current control structure block
            # execution_order<: List to hold statements in execution order
            self.current_block = []
            self.execution_order = []
        #
        def update_statement(self,statement: str) -> 'Parser.StatementHandler':
            self.statement = remove_comments_and_empty_lines(statement) 
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
        def handle_control_structure(self) -> 'Parser.StatementHandler' : 
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
        def handle_individual_statement(self) -> 'Parser.StatementHandler':
            # Add individual statements to the execution order, if not empty
            #
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
        #for statement in self.sql_query.split(';'):
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

    def parse_sql_code(self) -> list:
        sql_statements = self.split_on_control_structures()
        return sql_statements
    #

