#********** For testing
import os
import sys
#*******
import re
import pandas as pd
#**********
#********

#******* Start internimport
from Oracle import Oracle
#******* Slutt internimport



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
        self.dtypes = Oracle.get_classes_from_dtypes(dtypes)
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