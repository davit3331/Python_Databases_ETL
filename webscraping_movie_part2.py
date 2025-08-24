import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup



url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name2 = "Movies2.db"
table_name2 = 'Top_25'  
csv_path = 'top_25_films.csv' #A string that acts as the final csv file
df = pd.DataFrame(columns=["Film","Year","Rotten Tomatoes' Top 100"]) #Creating an empty dataframe
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser') #Data is a BeautifulSoup object. Which is like a structured tree. 


'''The line bellow finds all <tbody> tags in the HTML (table body elements). The result is a list of tables. The first table (table[0]) in this list, is the
table we need for this task'''  
tables = data.find_all('tbody')

'''Takes the first <tbody> (that’s why [0]). Finds all <tr> (table rows) inside of the first table.
Now rows is a list of all rows in that table (table[0] that we needed).'''
rows = tables[0].find_all('tr')

for row in rows:
    if count < 25:
        cols = row.find_all('td')
        if(len(cols) != 0):
            new_part_dataframe = pd.DataFrame([{"Film": cols[1].text,"Year": cols[2].text,"Rotten Tomatoes' Top 100": cols[3].text}])
            df = pd.concat([df, new_part_dataframe], ignore_index = True) 
            count += 1
    else:
        break

print(df)


df.to_csv(csv_path) #After the dataframe has been created, we save it to a CSV file, by creating a using the file path. 

#To store the required data in a database, you first need to initialize a connection to the database, save the dataframe as a table, and then close the connection
conn = sqlite3.connect(db_name2)
df.to_sql(table_name2, conn, if_exists='replace', index=False)
conn.close()
