import sqlite3
import pandas as pd

db_name = 'STAFF.db'
table_name = 'INSTRUCTORS'

conn = sqlite3.connect(db_name) ##conection has been opened, and given a database name

##we can do df.sql3 using the table_name we wanna give in the datbase, the connection to the database
#################################

attributes_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']
df = pd.read_csv('INSTRUCTOR.csv', names = attributes_list) ## Read CSV and replace default/absent headers with our own column names
print(df)

df.to_sql(table_name, conn, if_exists='replace', index = False) #Now the data is uploaded to the table in the database, anyone with access to the database can retrieve this data by executing SQL queries.
print('Table is ready')


#SQL queries can be executed on the data using the read_sql function in pandas.
#below is the first sql querry
query_statement = f"SELECT * FROM {table_name}" #Creating the query statement as a string
query_output = pd.read_sql_query(query_statement, conn) 

print(query_statement)
print(query_output) #printing the result of the sql quiery


#####Another SQl Query 
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

######Another SQL Query
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

##Adding a new row to the table in SQL
data_dict = {"ID": 100, "FNAME": "JOHN", "LNAME": "DOE", "City": "Paris", "CCODE": "FR"}
data_to_append = pd.DataFrame([data_dict])

data_to_append.to_sql(table_name, conn, if_exists='append', index=False)

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


conn.close()


