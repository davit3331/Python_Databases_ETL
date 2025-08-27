import pandas as pd
import glob
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

output_file = 'Countries_by_GDP.json'
db_name = 'World_econmies.db'
col_names = ['Country', 'GDP_USD_billion']
df = pd.DataFrame(columns = col_names)



url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

# Send an HTTP GET request to the server and store the response object
response = requests.get(url)

# Extract the HTML source code as a text string from the response
html_text = response.text

# Parse the HTML text into a structured "soup" object, (so we can easily navigate tables, headings, etc.)
data = BeautifulSoup(html_text, 'html.parser')

#find all the tables in data
tables = data.find_all('tbody')

#the table we need is at index 2 of the list of all tables
table = tables[2]                      # #this works better instead, table = data.find("table", {"class": "wikitable"})

#all the rows
rows = table.find_all("tr")

for row in rows:
    cols = row.find_all('td')
    if(len(cols) != 0):
        data_to_append_as_dict = {'Country':cols[0].get_text(strip=True).replace(u'\xa0', ' '), 'GDP_USD_billion':cols[2].text}
        df_to_append = pd.DataFrame([data_to_append_as_dict])
        df = pd.concat([df, df_to_append], ignore_index=True)

#rounding
# 1. Remove commas
df['GDP_USD_billion'] = df['GDP_USD_billion'].str.replace(',', '', regex=False)

# 2. Extract only the number part (ignore [1], notes, etc.)
df['GDP_USD_billion'] = df['GDP_USD_billion'].str.extract(r'([-+]?\d*\.?\d+)')[0]

# 3. Convert to float
df['GDP_USD_billion'] = df['GDP_USD_billion'].astype(float)

# 4. Round to 2 decimals
df['GDP_USD_billion'] = df['GDP_USD_billion'].round(2)

pd.set_option("display.float_format", "{:.2f}".format) #making sure it prints like this *.00 and not like this *.0



###converint dataframe to json file
df.to_json(output_file, double_precision=2)
print(df)


#converting to SQL       
table_name = 'Countries_by_GDP'
connection = sqlite3.connect(db_name)
df.to_sql(table_name, connection, if_exists='replace', index=False)


sql_statement = f"SELECT Country, GDP_USD_billion FROM {table_name} WHERE GDP_USD_billion > 100000"
print(pd.read_sql(sql_statement, connection))
