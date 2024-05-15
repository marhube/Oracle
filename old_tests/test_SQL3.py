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
from Oracle.PrepData import robust_get_data_type_from_values


import unittest

def get_lines_of_parsed_sql_code(sql_file_path):
    sql_parser = Parser(sql_file_path)
    sql_statements = sql_parser.parse_sql_code()
    sql_statements_lines = [item for  statement in sql_statements for item in statement.split('\n')]
    return sql_statements_lines

def read_stripped_lines(input_file):
    parsed_sql_file_path = input_file
    list_stripped_lines = []
    # Open the file for reading
    with open(parsed_sql_file_path, "r") as file:
        for line in file:
            # Add each line to the list, stripping the newline character
            list_stripped_lines.append(line.strip())

    return list_stripped_lines


class TestSQL(unittest.TestCase): 
    
    def test_remove_comments_and_empty_lines(self):
        print('Er nå inne i TestSQL.test_remove_comments_and_empty_lines som ikke er implementert')
    
    def test_generate_select(self):
        print('Er nå inne i TestSQL.test_generate_select')  
        table_name = 'MY_TABLE'
        select_table = SQL.generate_select(table_name)
        self.assertEqual(select_table,f'SELECT *  FROM {table_name}')
    
    def test_init_SQL_Generator(self):
        print('Er nå inne i TestSQL.test_init_SQL_Generator')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        sql_generator = SQL_Generator(test_data.dtypes,table_name=table_name,date_cols = date_cols)
        subresults = [
            sql_generator.date_cols == date_cols,
            isinstance(sql_generator,SQL_Generator)
            ]
             
        
        self.assertTrue(pd.Series(subresults).all())
    #
    
    def test_clean_name(self):
        print('Er nå inne i TestSQL.test_clean_name som ikke er implementert')
        
    
    def test_clean_table_name(self):
        print('Er nå inne i TestSQL.test_clean_table_name som ikke er implementert')
    
    def test_map2db_dtype(self):
        print('Er nå inne i TestSQL.test_map2db_dtype som ikke er ferdig implementert')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = generate_test_data(test_n,include_all_missing = True)
        test_data['date_only_na'] = test_data['datetime_only_na']
        data_types = robust_get_data_type_from_values(test_data)
        date_cols = [col for col in data_types if data_types[col] == "datetime.date"]
        date_cols.append('date_only_na')
        sql_generator = SQL_Generator(test_data.dtypes,table_name=table_name,date_cols = date_cols)        
        db_data_types = sql_generator.map2db_dtype()
        #Må lage en fasit
        db_data_types_comparison = {}
        db_data_types_comparison['float_with_na']  = 'NUMBER'
        db_data_types_comparison['float_no_na']  = 'NUMBER'
        db_data_types_comparison['int_with_na']  = 'INTEGER'
        db_data_types_comparison['int_no_na']  = 'INTEGER'
        db_data_types_comparison['datetime_with_na']  = 'TIMESTAMP'
        db_data_types_comparison['datetime_no_na']  = 'TIMESTAMP'
        db_data_types_comparison['date_with_na']  = 'DATE'
        db_data_types_comparison['date_no_na']  = 'DATE'
        db_data_types_comparison['str_with_na']  = 'VARCHAR2(255)'
        db_data_types_comparison['str_no_na']  = 'VARCHAR2(255)'
        db_data_types_comparison['float_only_na']  = 'NUMBER'
        db_data_types_comparison['int_only_na']  = 'INTEGER'
        db_data_types_comparison['datetime_only_na']  = 'TIMESTAMP'
        db_data_types_comparison['date_only_na']  = 'DATE'
        self.assertEqual(db_data_types,db_data_types_comparison)
        
    
    def test_init_SQL_Create(self):
        print('Er nå inne i TestSQL.test_init_SQL_Create')
        table_name = "MY_TABLE"
        test_n = 10 
        test_data = test_data = generate_test_data(test_n,include_all_missing = True)
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
        # and compare it with the previously parsed sql-code..
        sql_file_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/tls.sql"
        parsed_sql_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/tls_parsed.sql"
        sql_statements_lines = get_lines_of_parsed_sql_code(sql_file_path)        
        sql_statement_comparison = read_stripped_lines(parsed_sql_path)
        #             
        print(f'len(sql_statements_lines) er {len(sql_statements_lines)} og len(sql_statement_comparison) er {len(sql_statement_comparison)}')
        subresults = [sql_statements_lines[ind] == sql_statement_comparison[ind] for ind in range(len(sql_statements_lines))]
        subresults.append(len(sql_statements_lines) == len(sql_statement_comparison))
                
        self.assertTrue(pd.Series(subresults).all()) 
    
    def test_parse_complex_sql_code(self):
        print('Er nå inne i test_parse_complex_sql_code som ikke er implementert')
        # Ser nå på en mer komplisert sql-kode:
        sql_file_path= "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/ld.sql"
        #parsed_sql_path = "C:/Users/m01315/General_Python/Package/DB/test_data/SQL/tls_parsed.sql"
        sql_statements_lines = get_lines_of_parsed_sql_code(sql_file_path) 
        print(f'len( sql_statements_lines) er {len( sql_statements_lines)}')       
        #sql_statement_comparison = read_stripped_lines(parsed_sql_path)
        for enum,line in enumerate(sql_statements_lines):
            print(f'line {enum}: {line}')
        
        
    
        
        
        