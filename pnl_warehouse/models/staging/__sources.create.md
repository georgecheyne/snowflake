choose the Run operation command above
add the following as additional info:

generate_source --args '{"schema_name": "pnl", "database_name": "BLUECREST_STAGING", "table_names":["TRANSACTION_VALUE"], "generate_columns":"true"}'

the output will contain the sources definition