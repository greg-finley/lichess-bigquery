import csv

# Ad-hoc script needed because on first loads the games data types sometimes ended up as non-string types

table_catalog = "greg-finley"
table_schema = "lichess"

# Read the CSV file
with open("column.csv") as csvfile:
    reader = csv.reader(csvfile)
    # Skip the header row
    next(reader)
    # Group the rows by table name
    tables = {}
    for row in reader:
        if row[2] not in tables:
            tables[row[2]] = []
        tables[row[2]].append(row[3])

# Generate the SQL command for each table and write the commands to separate files
with open("commands.txt", "w") as f:
    for table_name, columns in tables.items():
        # Generate the SELECT statement to cast all columns to STRING
        select_stmt = ", ".join(
            [f"CAST({column} AS STRING) AS {column}" for column in columns]
        )
        # Generate the SQL command to overwrite the BigQuery table
        sql_cmd = f"CREATE OR REPLACE TABLE `{table_catalog}.{table_schema}.{table_name}` AS SELECT {select_stmt} FROM `{table_catalog}.{table_schema}.{table_name}`;\n"
        f.write(sql_cmd)
