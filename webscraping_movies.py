import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'  
csv_path = 'top_50_films.csv' #A string that acts as the final csv file
df = pd.DataFrame(columns=["Average Rank","Film","Year"]) #Creating an empty dataframe
count = 0


html_page = requests.get(url).text #Using the url to get the html_page as a text
data = BeautifulSoup(html_page, 'html.parser') #Data is a BeautifulSoup object. Which is like a structured tree. 


'''The line bellow finds all <tbody> tags in the HTML (table body elements). The result is a list of tables. The first table (table[0]) in this list, is the
table we need for this task'''  
tables = data.find_all('tbody') 

'''Takes the first <tbody> (that’s why [0]). Finds all <tr> (table rows) inside of the first table.
Now rows is a list of all rows in that table (table[0] that we needed).'''
rows = tables[0].find_all('tr')



for row in rows:  #loops over each table row
    if count < 50: #Stops after 50 rows, since you only want top 50 films
        col = row.find_all('td') #finds all the table data cells in that row

        if(len(col) != 0): #makes sure the row isn’t empty (sometimes header rows don’t contain <td>).
            df = pd.concat([df, pd.DataFrame([{"Average Rank":col[0].text, "Film": col[1].text, "Year":col[2].text}])], ignore_index = True)
            count += 1
    else: 
        break

'''#The code given by the course, does the same thing as my code above but written slighly differently
for row in rows:
    if count<50:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Average Rank": col[0].contents[0],
                         "Film": col[1].contents[0],
                         "Year": col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            count+=1
    else:
        break
'''
print(df)

df.to_csv(csv_path) #After the dataframe has been created, we save it to a CSV file, by creating a using the file path. 

#To store the required data in a database, you first need to initialize a connection to the database, save the dataframe as a table, and then close the connection
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
