import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 


log_file = "log_file_2.txt"
targetCsvFile = "transformed_data.csv"

#Extraction Functions - turning respective csv, json, xml files into regular dataframes using pandas

# NOTE:
# We're concatenating into an empty DataFrame inside a loop.
# This works and is fine for the assignment, but pandas gives a FutureWarning
# because this pattern may behave differently in future versions.
# A more efficient approach in real projects would be:
# 1) Collect rows/dataframes in a list
# 2) Call pd.concat() once at the end


def extractFromCsv(csv_file_to_process):
    dataframe = pd.read_csv(csv_file_to_process)
    return dataframe

def extractFromJson(json_file_to_process):
    dataframe = pd.read_json(json_file_to_process, lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns = ["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        car_model = person.find("car_model").text
        year_of_manufacture = float(person.find("year_of_manufacture").text)
        price = float(person.find("price").text)
        fuel = person.find("fuel").text
        dataframe = pd.concat([dataframe, pd.DataFrame([{"car_model":car_model, "year_of_manufacture": year_of_manufacture, "price":price, "fuel": fuel}])], ignore_index = True)
    return dataframe


def extract():

    dataframe = pd.DataFrame(columns = ["car_model", "year_of_manufacture", "price", "fuel"])

    for csvfile in glob.glob("*.csv"):
        if csvfile != targetCsvFile:
            extracted_csv_dataFrame = extractFromCsv(csvfile) #we turn the csv file into a dataframe
            dataframe = pd.concat([dataframe, extracted_csv_dataFrame], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extracted_json_dataFrame = extractFromJson(jsonfile)  #we turn the json file into a dataframe
        dataframe = pd.concat([dataframe, extracted_json_dataFrame], ignore_index=True)

    
    for xmlfile in glob.glob("*.xml"):
        extracted_xml_dataFrame = extract_from_xml(xmlfile)    #we turn the xml file into a dataframe
        dataframe = pd.concat([dataframe, extracted_xml_dataFrame], ignore_index=True)

    return dataframe


def transform(dataframe_to_transform):
    dataframe_to_transform["price"] = dataframe_to_transform["price"].round(2)
    return dataframe_to_transform


def load(targetCsvFile, data):
    data.to_csv(targetCsvFile)


# Logging
def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 
    

# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the begining of extraction face
log_progress("Extract Phase Started")
extracted_data = extract()

# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 


# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print(transformed_data)

# Log the completion of the transformation process 
log_progress("Transform phase Ended") 


# Log the begining of the load phase
log_progress("Load phase started")
load(targetCsvFile, transformed_data)

# Log the completion of the load phase
log_progress("load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")

