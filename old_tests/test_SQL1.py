import os
import sys
import re
import pandas as pd
#**********
#********



#Forsøk på  systematisk testing
from TestData import generate_test_data
import Oracle.SQL as SQL
from Oracle.SQL import SQL_Generator,SQL_Create,Parser

import unittest

def get_lines_of_parsed_sql_code(sql_file_path):
    sql_parser = Parser(sql_file_path)
    sql_statements = sql_parser.parse_sql_code()
    sql_statements_lines = [item for  statement in sql_statements for item in statement.split('\n')]
    return sql_statements_lines

def compare_sql_code(sql_file1,sql_file2):
    pass



class TestSQL(unittest.TestCase): 
    
    def test_generate_select(self):
        print('Er nå inne i TestSQL.test_generate_select')  
        table_name = 'MY_TABLE'
        select_table = SQL.generate_select(table_name)
        self.assertEqual(select_table,f'SELECT *  FROM {table_name}')
    
    def test_remove_comments_and_empty_lines(self):
        print('Er nå inne i TestSQL.test_remove_comments_and_empty_lines som ikke er implementert')
    
    
    def test_init_SQL_Generator(self):
        print('Er nå inne i TestSQL.test_init_SQL_Generator')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n)
        sql_generator = SQL_Generator(test_data.dtypes,table_name=table_name)
        self.assertIsInstance(sql_generator,SQL_Generator)
    #
    
    def test_clean_name(self):
        print('Er nå inne i TestSQL.test_clean_name som ikke er implementert')
        
    
    def test_clean_table_name(self):
        print('Er nå inne i TestSQL.test_clean_table_name som ikke er implementert')
    
    def test_map2db_dtype(self):
        print('Er nå inne i TestSQL.test_map2db_dtype som ikke er implementert')
        
    
    def test_init_SQL_Create(self):
        print('Er nå inne i TestSQL.test_init_SQL_Create')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n)
        sql_generator = SQL_Generator(test_data.dtypes,table_name=table_name)
        sql_create = SQL_Create(sql_generator)       
        self.assertIsInstance(sql_create,SQL_Create)

    def test_sql_create_table(self):
        print('Er nå inne i TestSQL.test_sql_create_table')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n)
        sql_generator = SQL_Generator(test_data.dtypes,table_name=table_name)
        sql_create = SQL_Create(sql_generator)
        sql_create_table = sql_create.sql_create_table()
        print('sql_create_table er')
        print(sql_create_table)
        #
        sql_comparison = '''
        CREATE PRIVATE TEMPORARY TABLE ORA$PTT_MY_TABLE (
        "float_with_na" NUMBER,
        "float_no_na" NUMBER,
        "int_with_na" INTEGER,
        "int_no_na" INTEGER,
        "datetime_with_na" DATE,
        "datetime_no_na" DATE,       
        "date_with_na" VARCHAR2(255),
        "date_no_na" VARCHAR2(255),
        "str_with_na" VARCHAR2(255),
        "str_no_na" VARCHAR2(255)
        ) ON COMMIT PRESERVE DEFINITION        
        '''
        stripped_sql_create =  re.sub(sql_create_table.strip(),"\n"," ")
        stripped_sql_comparison =  re.sub(sql_comparison.strip(),"\n"," ")
        self.assertEqual(stripped_sql_create,stripped_sql_comparison)   
    
    #Memo til selv: Bør teste også Parser!!!!!!!
    
    def test_parse_sql_code(self):
        print('Er nå inne i TestSQL.test_parse_sql_code')
        #Fetch original code and previously parsed sql-code. Then parse the original code
        # and compare it with the previously parsed sql-code.
        sql_file_path= "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/tls.sql"
        sql_parser = Parser(sql_file_path)
        sql_statements = sql_parser.parse_sql_code()
        sql_statements_lines = [item for  statement in sql_statements for item in statement.split('\n')]
        # 
        print(f'len(sql_statements) er {len(sql_statements)}')
        print(f'len(sql_statements_lines) er {len(sql_statements_lines)}')
        #for statement in sql_statements:
        #    print(statement)
        parsed_sql_file_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/tls_parsed.sql"
        sql_statement_comparison = []
        # Open the file for reading
        with open(parsed_sql_file_path, "r") as file:
            for line in file:
                # Add each line to the list, stripping the newline character
                sql_statement_comparison.append(line.strip())
        #
        print(f'len(sql_statement_comparison) er {len(sql_statement_comparison)}')
        print('sql_statement_comparison er')
        print(sql_statement_comparison)
        # Memo til selv: Flatlegger slik at hvert element svarer til en enkelt linje
       
        print(f'len(sql_statements_lines) er {len(sql_statements_lines)} og len(sql_statement_comparison) er {len(sql_statement_comparison)}')
        subresults = [sql_statements_lines[ind] == sql_statement_comparison[ind] for ind in range(len(sql_statements_lines))]
        subresults.append(len(sql_statements_lines) == len(sql_statement_comparison))
        # Ser nå på en mer komplisert sql-kode
        sql_file_kca = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/kca.sqll"
        sql_parser_kca = Parser(sql_file_kca)
        sql_statements_kca = sql_parser.parse_sql_code()
        sql_statements_lines = [item for  statement in sql_statements for item in statement.split('\n')]
        
        self.assertTrue(pd.Series(subresults).all()) 
    
    def test_parse_complex_sql_code():
        print('Er nå inne i test_parse_complex_sql_code')
        sql_file_path= "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/kca.sqll"
        sql_parser = Parser(sql_file_path)
        sql_statements = sql_parser.parse_sql_code()
        sql_statements_lines = [item for  statement in sql_statements for item in statement.split('\n')]
        
        
        
        