import pandas as pd
import glob
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime 

output_file = 'Countries_by_GDP.json'
db_name = 'World_econmies.db'
log_file = 'etl_project_log.txt'
table_name = 'Countries_by_GDP'
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

def get_html_as_BeautifulSoup(url):
    # Send an HTTP GET request to the server and store the response object
    response = requests.get(url)

    # Extract the HTML source code as a text string from the response
    html_text = response.text

    # Parse the HTML text into a structured "soup" object, (so we can easily navigate tables, headings, etc.)
    data = BeautifulSoup(html_text, 'html.parser')
    return data

def extract_DataFrame_table_from_html(Beautiful_soup_data):
    col_names = ['Country', 'GDP_USD_billion']
    df = pd.DataFrame(columns = col_names)
    
    #find the table we need in the data
    table = Beautiful_soup_data.find("table", {"class": "wikitable"})

    #all the rows
    rows = table.find_all("tr")

    for row in rows:
        cols = row.find_all('td')
        if(len(cols) != 0):
            data_to_append_as_dict = {'Country':cols[0].get_text(strip=True).replace(u'\xa0', ' '), 'GDP_USD_billion':cols[2].text}
            df_to_append = pd.DataFrame([data_to_append_as_dict])
            df = pd.concat([df, df_to_append], ignore_index=True)
    
    return df

#rounding
def transforming(dataframe):

    # 1. Remove commas
    dataframe['GDP_USD_billion'] = dataframe['GDP_USD_billion'].str.replace(',', '', regex=False)

    # 2. Extract only the number part (ignore [1], notes, etc.)
    dataframe['GDP_USD_billion'] = dataframe['GDP_USD_billion'].str.extract(r'([-+]?\d*\.?\d+)')[0]

    # 3. Convert to float
    dataframe['GDP_USD_billion'] = dataframe['GDP_USD_billion'].astype(float)

    # 4. Round to 2 decimals
    dataframe['GDP_USD_billion'] = dataframe['GDP_USD_billion'].round(2)

    pd.set_option("display.float_format", "{:.2f}".format) #making sure it prints like this *.00 and not like this *.0

    return dataframe



###converint dataframe to json file
def load_dataframe_toJSON(dataframe):
    dataframe.to_json(output_file, orient="records", indent=2, double_precision=2)


#converting to SQL       
def load_data_to_sql(dataframe, connection):
    dataframe.to_sql(table_name, connection, if_exists='replace', index=False)


def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

log_progress("ETL process has started")

log_progress('Starting the extraction process')
beautiful_soup_data = get_html_as_BeautifulSoup(url)

dataframe = extract_DataFrame_table_from_html(beautiful_soup_data)
log_progress('Extraction process ended')

log_progress('Transform process has started')
dataframe_transformed = transforming(dataframe)
load_dataframe_toJSON(dataframe_transformed)
log_progress('Transform process has ended')


log_progress('Load process has started')
connection = sqlite3.connect(db_name)
load_data_to_sql(dataframe_transformed, connection)
log_progress('Load process has ended')

log_progress("ETL process has finished")

sql_statement = f"SELECT Country, GDP_USD_billion FROM {table_name} WHERE GDP_USD_billion > 100000"

print(pd.read_sql(sql_statement, connection))
connection.close()


