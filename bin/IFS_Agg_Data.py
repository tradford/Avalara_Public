import os
import pandas as pd
import json
import pyodbc
import sqlalchemy as db
import urllib
import time
import requests
from logMessage import logMessage as lm


st = time.time()



df = pd.read_excel(r'excel_path', sheet_name='Sales Tax Report')


df2 = df.rename(columns=df.iloc[5]).drop(labels=[0,1,2,3,4,5], axis=0)
#print(df2)

new_df = df2.loc[df2['Code'] == 'CITY1']
#print(new_df.loc[new_df['Invoice']== '202300635'])
#print(new_df)
new_df['DOC_CODE'] = new_df['Series'] + new_df['Invoice'].astype(str)
#print(new_df['DOC_CODE'])
#new_df[new_df['Tax'] == '-'] = 0
new_df.replace("-", "0.0", inplace=True)
new_df["Tax"] = new_df["Tax"].apply(pd.to_numeric)
new_df["Gross"] = new_df["Gross"].apply(pd.to_numeric).round(decimals=2)

print(new_df)

dff = new_df.groupby(["Invoice", "DOC_CODE"]).Gross.sum().reset_index()
dff2 = new_df.groupby(["Invoice", "DOC_CODE"]).Tax.sum().reset_index()
# dff2.replace(0, 0.0, inplace=True)
print(dff2)
#frames = [dff, dff2]

#result = pd.concat(frames)

          
#dff = dff.groupby(["Invoice", "DOC_CODE", "Gross"]).Tax.sum().reset_index()
#dff = new_df.groupby(["Invoice","DOC_CODE"]).sum().reset_index()
# for index, row in dff.iterrows():
#     print(row, '\n')
# if dff['Tax'] == '-' and dff[]:
#         dff['Invoice']

# dff = dff[dff.Tax != '-']
#print(dff) 
print(dff.loc[dff['DOC_CODE']== 'CD202300001'])
print(dff2.loc[dff['DOC_CODE']== 'CD202300001'])
#print(result.loc[dff['DOC_CODE']== 'CD202300001'])
os.chdir(r'path')
with pd.ExcelWriter('10012022_10252022_py.xlsx') as writer:
    dff.to_excel(writer, sheet_name="IFS_PY")
    dff2.to_excel(writer, sheet_name="IFS_PY_tax")

# end = time.time()
# run_time = end - st
# minutes = run_time / 60
# message = (
#     "The Transform and Load process for the table table took "
#     + str(minutes)
#     + " minutes to complete"
# )
# lm(r"path", message)