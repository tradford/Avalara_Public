import os
import pandas as pd
import json
import pyodbc
import sqlalchemy as db
import urllib
import time

from logMessage import logMessage as lm


st = time.time()



df2 = pd.read_excel(r'excel_path', sheet_name='IFS_PY')



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
#print(result_set)
dff = pd.DataFrame(result_set)
# cnxn = pyodbc.connect(r'DRIVER=driver;SERVER=Server;DATABASE=database;Trusted_Connection=yes;')
# cursor = cnxn.cursor()
# #cursor = conn_str.cursor()
# query = "SELECT * FROM Database.schema.table;"
# dff = pd.read_sql(query, cnxn)
# #print(type(dff))
#print(dff.head(26))

# json_file = "merge_test_facts_Extended.json"

## Script dir ##
# scrpt_dir = os.path.dirname(os.path.abspath(__file__))

## Read data ##
# with open(f"{scrpt_dir}\{json_file}", "r") as file:
#with open(json_file, "r") as file:
#    data = json.load(file)

    ## Pandas dataframe + SQL ##

    # df = pd.json_normalize(data).astype(str)

#print(type(df))
df2.rename(columns={'DOC_CODE': 'code'}, inplace=True)
frames = [df2, dff]

#result = df.append(dff)
result = pd.concat(frames)
#print(type(result))
#newdf = pd.DataFrame(result)
new_df = result.astype(str)
#new_df.rename["DOC_CODE"]  = new_df["code"]
print(list(new_df.columns))

trunc = new_df[["code", "Gross", "totalAmount"]]
#new_dff = new_df.drop_duplicates()
# for names in column_names:
#tmp = df.explode("Contributing Teams.value")
#dff = pd.DataFrame(tmp)
#tmp2 = dff.explode("Business Owners.value")
#new_dff = tmp2

#new_dff.to_sql("Table", con=engine, if_exists="replace", index=False)
os.chdir(r'directory')

trunc.to_excel("excel_file", sheet_name="All_PY")
end = time.time()
run_time = end - st
minutes = run_time / 60
message = (
    "The Transform and Load process for the Table table took "
    + str(minutes)
    + " minutes to complete"
)
#lm(r"log file", message)



#new_dff = new_df.drop_duplicates()
