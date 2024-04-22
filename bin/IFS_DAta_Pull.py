import oracle_connect

query = 'procedure_name'
# Parse Data
orc = oracle_connect.Oracle()
orc.connect_node()
try:
    orc.execute_proc_node(sql=query, commit=True)
finally:
    orc.disconnect_node()