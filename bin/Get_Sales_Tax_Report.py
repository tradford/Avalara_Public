from datetime import datetime as dt
import os
import calendar
import oracle_connect2 as oracle_connect
import pandas as pd
import numpy as np

#from datetime import date
from dateutil.relativedelta import relativedelta


'''The following code is used to create a usable excel file for accounting. the procedure is as follows:
    1) create date variables to be used in a select query
    2) connect to an oracle database and execute the select query and return the data to be used in the excel file
    3) modify the data so it is optimized for the needs of accounting
    4) save the excel file to a shared directory
    '''
    
# Step 1     create date variables to be used in a select query
today = dt.today()
#################################

if today.strftime('%m') == '01':
    prevs_mnth_day1 = today.replace(year=today.year - 1, month=12, day=1).strftime('%d/%m/%Y').upper()
    prevs_last_day_mnth = today.replace(year=today.year - 1, month=12, day=31).strftime('%d/%m/%Y').upper()
else:
    prevs_mnth = today + relativedelta(months=-1)
    prevs_mnth_cor = prevs_mnth.strftime('%d/%b/%Y').upper()
    prevs_year = today + relativedelta(years=-1)
    prevs_year_cor = prevs_year.strftime('%d/%b/%Y').upper()
    just_prevs_mnth = prevs_mnth.strftime('%m')
    just_prevs_year = prevs_year.strftime('%Y')
    prevs_mnth_day1 = prevs_mnth.replace(day=1).strftime('%d/%m/%Y').upper()
    prevs_last_day_tup = calendar.monthrange(int(just_prevs_year), int(just_prevs_mnth))
    prevs_last_day = prevs_last_day_tup[1]
    prevs_last_day_mnth = prevs_mnth.replace(day=prevs_last_day).strftime('%d/%m/%Y').upper()
 
#print(prevs_mnth_day1)   
#print(prevs_last_day_mnth)
#################################################

just_mnth = today.strftime('%m')
just_year = today.strftime('%Y')
mnth_day1 = dt.today().replace(day=1)
last_day_tup = calendar.monthrange(int(just_year), int(just_mnth))
last_day = last_day_tup[1]
#####################################################

nxt_mnth = today + relativedelta(months=1)
nxt_year = today + relativedelta(years=1)
just_nxt_year = nxt_year.strftime('%Y')
just_nxt_mnth = nxt_mnth.strftime('%m')
nxt_mnth_day1 = nxt_mnth.replace(day=1)
nxt_last_day_tup = calendar.monthrange(int(just_nxt_year), int(just_nxt_mnth))
nxt_last_day = nxt_last_day_tup[1]
nxt_last_day_mnth = nxt_mnth.replace(day=nxt_last_day).strftime('%d/%m/%Y').upper()

print(nxt_mnth_day1.strftime('%d/%m/%Y').upper())
# print(just_year)
# print(just_nxt_mnth)
# print(last_day[1])
# x = dt.strptime(today, '%Y-%m-%d')

# print(x.strftime('%d-%b-%Y').upper())

# Step 2 connect to an oracle database
query = f'''SELECT
            *
FROM 
            table
            Where condition '''

#print(query)
# # Parse Data
lib_dir = r"oracle_path"
os.chdir(lib_dir)
orc = oracle_connect.Oracle()
orc.connect_node()

try:
    orc.execute_node(sql=query, commit=False)
    data = orc.cursor.fetchall()
    col_names = []
    for i in range(0, len(orc.cursor.description)):
        col_names.append(orc.cursor.description[i][0])
    dataset = pd.DataFrame(data)
finally:
    orc.disconnect_node()

# Step 3 modify the data so it is optimized for the needs of accounting
dataset.columns = col_names
#print(dataset.head())
os.chdir(r'dir')
df = dataset.loc[(dataset['CODE'] != 'STATE1') & (dataset['CODE'] != 'COUNTY1') & (dataset['CODE'] != 'DISTRICT1') & (dataset['SERIES'] != 'II')]
#print(df.keys())
dfs = dataset.loc[dataset['SERIES'] == 'II']
df['CUST_STATE'] = df['CUST_STATE'].str.upper()
dff = df.sort_values(by=['CUST_STATE'])

frames = [dff, dfs]
result3 = pd.concat(frames)
#print(result3.head())
result4 = result3.reset_index()
dftemp = result4.sort_values('CUST_STATE')
dft = dftemp.pivot_table(index='CUST_STATE',
               margins=True,
               margins_name='total',  # defaults to 'All'
               aggfunc=sum)
#print(dft.head(100))
#new_dates=dft.index.set_names("STATE")
#dft.index=new_dates
dft['ID'] = range(1, len(dft) + 1)
dft = dft.reset_index()

#dftt = dft.set_index('ID', inplace=True)
#print(dft.head())
frames = [dff, dft, dfs]

#result = df.append(dff)
result1 = pd.concat(frames)
#print(result1.head())
result = result1.reset_index()
dfr = result.sort_values(['CUST_STATE','NET'])

#print(dfr.head(100))
#dffr =pd.DataFrame(dfr)
#dvr = dffr.to_dataframe()
#print(dffr.head())


#dfr = dfr.reset_index()
#dfr.style.hide_index()
#dfr.drop(dfr.columns[0], axis=1, inplace=True)
dfr.drop(['index', 'level_0'], axis=1, inplace=True)
# def bold_style(val):
#     f = "font-weight: bold" 
#     #condition
#     m = dfr["NAME"] == None
#     # DataFrame of styles
#     df1 = pd.DataFrame('', index=val.index, columns=val.columns)
#     # set columns by condition
#     df1 = df1.mask(m, f)
#     return df1

#print(pd.isnull(dfr.loc['NAME']))
#dfrr=dfr.style.apply(bold_style, axis=None)
#print(dfr.loc[dfr['INVOICE']=='202300294'])
#dfr = dfr.drop_duplicates()
#print(dfr.loc[dfr['INVOICE']=='202300294'])
state = dfr['CUST_STATE']
dfr.drop(['CUST_STATE'], axis=1, inplace=True)
dfr.insert(loc=1, column='CUSTOMER_STATE', value=state)
#print(dfr.keys())
dfr.INVOICE.fillna(value=dfr['CUSTOMER_STATE'] +' TOTAL', inplace=True)
dfr.style.applymap(lambda x: 'font-weight: bold' if x == dfr['CUSTOMER_STATE'] +' TOTAL' else '', axis=1, inplace=True)
#print(dfr.keys())
#print(dfr.head())

# Step 4 save the excel file to a shared directory
prevs_mnth_day1 = prevs_mnth.strftime('%B')
title = f"file_{prevs_mnth_day1}.xlsx"
dfr.to_excel(title, sheet_name='Sales Tax Report')
## with pd.excel_file') as writer:
#    #dff.to_excel(writer, sheet_name="Sales Tax Report")