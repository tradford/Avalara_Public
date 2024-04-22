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
    'Authorization': 'Auth'
    }

    response = requests.request("GET", url, headers=headers, data=payload, files=files)

    j_response = response.json()
    ##print(j_response)
    jj_response = j_response['value']
    print(jj_response)
     
    lm(r"path", "Successfully called API" )
except Exception as e:
    lm(r"path", "Error: " + str(e))
#new_response = json.load(j_response)
#print(jj_response)
## SQL connection ##

## JSON file ##


conn_str = "DRIVER={Driver};" + "SERVER=Server;DATABASE=Database"
quoted = urllib.parse.quote_plus(conn_str)
engine = db.create_engine(f"mssql+pyodbc:///?odbc_connect={quoted}")

connection = engine.connect()
metadata = db.MetaData()
trans = db.Table('table', metadata, autoload=True, autoload_with=engine)
query = db.select([trans]) 
result_proxy = connection.execute(query)
result_set = result_proxy.fetchall()
print(result_set)
dff = pd.DataFrame(result_set)

df = pd.json_normalize(jj_response)
#print(type(df))
frames = [df, dff]
#result = df.append(dff)
result = pd.concat(frames)
#print(type(result))
#newdf = pd.DataFrame(result)
new_df = result.astype(str)
new_dff = new_df.drop_duplicates()

new_dff["id"] = new_dff["id"].apply(pd.to_numeric)
old_data1 = new_dff[new_dff.duplicated(['code'])]
print(old_data1)
#old_data = old_data1[old_data1['id'].max()]
try:
  old_data1.loc[old_data1['id'].idxmax()]
  old_data2 = old_data1[old_data1['id'] < old_data1]
  new_dff.drop(old_data2.index, inplace = True)
  new_dff.to_sql("table", con=connection, if_exists="replace", index=False)
except ValueError:
    print("hey")
#print(new_dff.loc[new_dff['code'] == 'CD202300259'])

    new_dff.to_sql("table", con=connection, if_exists="replace", index=False)

end = time.time()
run_time = end - st
minutes = run_time / 60
message = (
    "The Transform and Load process for the Table table took "
    + str(minutes)
    + " minutes to complete"
)
lm(r"path", message)



