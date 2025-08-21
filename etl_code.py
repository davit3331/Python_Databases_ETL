import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

""" 
2 files paths available globbaly for all functions. the log_file and the target file
to tstore the final output data that we can load into a database.
"""
log_file = "log_file.txt"
target_file = "transformed_data.csv"


#EXTRACTION PROCESS

#Extracting CSV file
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

#Extracting JSON file
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines = True) 
    return dataframe


#Extracting xml file
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height": height, "weight":weight}])], ignore_index = True)
    return dataframe



def extract():

    #create an empty data frame
    dataframe = pd.DataFrame(columns = ["name", "height", "weight"])

    #process all csv files, except the target file
    for csvfile in glob.glob("*.csv"):

        if csvfile != target_file: #Checking if the file is not the target file
            extracted_csv = extract_from_csv(csvfile)
            dataframe = pd.concat([dataframe, extracted_csv], ignore_index = True)

    #process all json files
    for jsonFile in glob.glob("*.json"):
        extracted_json = extract_from_json(jsonFile)
        dataframe = pd.concat([dataframe, extracted_json], ignore_index = True)

    #process all xml files
    for xmlFile in glob.glob("*.xml"):
        extracted_xml = extract_from_xml(xmlFile)
        dataframe = pd.concat([dataframe, extracted_xml], ignore_index = True)

    
    return dataframe

    
#Transform
def transform(data):
    data["height"] = (data["height"] * 0.0254).round(2)
    data["weight"] = (data["weight"] * 0.45359237).round(2)
    return data


# Loading
def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file)


# Logging
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 



################################################################################################
#################################### TESTING ###################################################


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
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the begining of the load phase
log_progress("Load phase started")
load_data(target_file, transformed_data)

# Log the completion of the load phase
log_progress("load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")