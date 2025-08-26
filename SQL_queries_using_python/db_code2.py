import sqlite3
import pandas as pd
from db_code import table_name, db_name

table2_name = "Departments"
connection = sqlite3.connect(db_name)  #new connection to the same database as in db_code.py

cols = ["DEPT_ID", "DEP_NAME", "MANAGER_ID", "LOC_ID"] #col names for the new table

df = pd.read_csv("Departments.csv", names = cols)  #loading the new table from csv to a dataframe, with the col names

df.to_sql(table2_name, connection, if_exists='replace', index=False) #loading the dataframe into our sql database

data_append_dict = {"DEPT_ID":9, "DEP_NAME": "Quality Assurance", "MANAGER_ID":30010, "LOC_ID":"L0010"}  #the data to append


data_frame_to_append = pd.DataFrame(data_append_dict, index=[0]) #making the data to append from dict to dataframe

data_frame_to_append.to_sql(table2_name, connection, if_exists='append', index=False)   

print("sql query 1")
sql_statement = f"SELECT * FROM {table2_name}"
print(pd.read_sql(sql_statement, connection))
print('\n')

print("sql query 2")
sql_statement_2 = f"SELECT DEP_NAME FROM {table2_name}"
print(pd.read_sql(sql_statement_2, connection))
print('\n')

print("sql query 3")
sql_statement_3 = f"SELECT COUNT(*) FROM {table2_name}"
print(pd.read_sql(sql_statement_3, connection))
print('\n')

connection.close()