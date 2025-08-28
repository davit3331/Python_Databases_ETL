import pandas as pd
import glob
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime 

def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

def extract(url: str, column_names:list) -> pd.DataFrame:

    column_names = ['Name', 'MC_USD_Billion']

    response = requests.get(url)
    if response.status_code == 200:
            html_text = response.text
            data = BeautifulSoup(html_text, 'html.parser')
        
            bank_dataframe = pd.DataFrame(columns=column_names)

            tables = data.find_all('tbody')
            table = tables[0]

            rows = table.find_all('tr')

            for row in rows:
                cols = row.find_all('td')
                if(len(cols) != 0):
                        dict_to_add = {'Name': cols[1].text.strip(), 'MC_USD_Billion': cols[2].text.strip()}
                        dataframe_to_add = pd.DataFrame(dict_to_add, index=[0])
                        bank_dataframe = pd.concat([bank_dataframe, dataframe_to_add], ignore_index=True)

            return bank_dataframe
        
    else:
        print(f"Failed to retrieve content. Status code: {response.status_code}")
        html_text = None
        #No need to add return None, not having it is the same thing
            


def transform(extracted_dataframe: pd.DataFrame, csv_file : str) -> pd.DataFrame:

    ##bellow we are reading the CSV file, and storing the conversion rates from it
    #csv_file = glob.glob('exchange_rate.csv')[0]
    exchange_rate_dataframe = pd.read_csv(csv_file) #reading the exchange rates from the csv file, storing it in a dataframe
    
    exchange_rate_dataframe = exchange_rate_dataframe.set_index('Currency') #the dataframe has indexes, we change them to be 'Currency'
   
    EUR_conversion = exchange_rate_dataframe.loc['EUR', 'Rate']
    GBP_conversion = exchange_rate_dataframe.loc['GBP', 'Rate']
    INR_conversion = exchange_rate_dataframe.loc['INR', 'Rate']

    df = extracted_dataframe.copy()

    #below we are making the MC_USD_Billion column into a float in 4 steps
    # 1. Remove commas
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(',', '', regex=False)

    # 2. Extract only the number part (ignore [1], notes, etc.)
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.extract(r'([-+]?\d*\.?\d+)')[0]

    # 3. Convert to float
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

    # 4. Round to 2 decimals
    df['MC_USD_Billion'] = df['MC_USD_Billion'].round(2)

    pd.set_option("display.float_format", "{:.2f}".format) #making sure it prints like this *.00 and not like this *.0

    
    ###adding new columns to our dataset with their respective conversion
    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * EUR_conversion).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * GBP_conversion).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * INR_conversion).round(2)

    return df

def load_to_csv(df: pd.DataFrame, output_path: str):
     df.to_csv(output_path)


def load_to_db(dataframe : pd.DataFrame, sql_connection, table_name: str, ):
    dataframe.to_sql(table_name, connection, if_exists='replace', index = False)
    




def run_query(sql_statement: str, connection: sqlite3) -> pd.DataFrame:
     print(pd.read_sql(sql_statement, connection))
     
###############

log_file = 'code_log.txt'

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
column_names = ['Name', 'MC_USD_Billion']
log_progress('Preliminaries complete. Initiating ETL process')

extracted_dataframe = extract(url, column_names)
print(extracted_dataframe)
log_progress('Data extraction complete. Initiating Transformation process')


transformed_dataframe = transform(extracted_dataframe, 'exchange_rate.csv')

log_progress('Data transformation complete. Initiating Loading process')
print(transformed_dataframe['MC_EUR_Billion'][4])




db_name = 'Banks.db'
table_name = 'Largest_banks'

load_to_csv(transformed_dataframe, './Largest_banks_data.csv')
log_progress('Data saved to CSV file')



connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')


load_to_db(transformed_dataframe, connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')


sql_statement_1 = f"SELECT * FROM Largest_banks"
run_query(sql_statement_1, connection)

sql_statement_2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(sql_statement_2, connection)

sql_statement_3 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(sql_statement_3, connection)

log_progress('Process Complete')



log_progress('Server Connection closed')
connection.close

