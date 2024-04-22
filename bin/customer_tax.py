import json
import os
import time
import urllib

import pandas as pd
import pyodbc
import requests
import sqlalchemy as db
from logMessage import logMessage as lm

st = time.time()


try:
    
    
    url = "url"

    payload={}
    files={}
    headers = {
    'Authorization': 'auth'
    }

    response = requests.request("GET", url, headers=headers, data=payload, files=files)

    j_response = response.json()
    total_count = j_response['@recordsetCount']
    total_count_int = int(total_count)
    
    
    
#     jj_response = j_response['value']
#     print(jj_response)
    lm(r"lag_path", "Successfully called Cert API" )
except Exception as e:
    lm(r"log_path", "Error In Cert: " + str(e))
    
try:
    all_data = pd.DataFrame()
    url_base = "url"

    payload = {}
    files = {}
    headers = {
        'Authorization': 'auth'
    }
    # Loop from 0 to total_count_int, stepping by 100
    for skip in range(0, total_count_int, 100):
        # Complete URL with the skip parameter
        url = url_base + str(skip)

        response = requests.request("GET", url, headers=headers, data=payload, files=files)

        # Check if the request was successful
        if response.status_code == 200:
            # Assuming the response is JSON, you can convert it to a DataFrame
            j_response2 = response.json()
            jj_response2 = j_response2['value']
            data = pd.json_normalize(jj_response2)

            # Append the data to the results
            all_data = pd.concat([all_data, data], ignore_index=True)
        else:
            print(f"Failed to fetch data for skip={skip}")

except Exception as e:
    lm(r"path", "Error In Cert: " + str(e))


# ## SQL connection ##


conn_str = "DRIVER={Driver};" + "SERVER=Server;DATABASE=Database"
quoted = urllib.parse.quote_plus(conn_str)
engine = db.create_engine(f"mssql+pyodbc:///?odbc_connect={quoted}")

connection = engine.connect()
metadata = db.MetaData()


# # Define the columns that contain nested objects
nested_columns = ['customers'] 

for column in nested_columns:
    # Explode the column, if it contains lists, into multiple rows
    df = all_data.explode(column)

    # Normalize the column into a flat table
    normalized = pd.json_normalize(df[column])

    # Drop the original nested column
    df = df.drop(columns=[column])

    # Concatenate the normalized data with the original DataFrame
    df = pd.concat([df.reset_index(drop=True), normalized.reset_index(drop=True)], axis=1)

# Now you should be able to write to SQL without errors
df.to_sql("table", con=engine, if_exists="replace", index=False)


end = time.time()
run_time = end - st
minutes = run_time / 60
message = (
    "The Transform and Load process for the Table table took "
    + str(minutes)
    + " minutes to complete"
)
lm(r"path", message)



