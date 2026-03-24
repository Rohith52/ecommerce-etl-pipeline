import pandas as pd
from sqlalchemy import create_engine
import os


engine = create_engine(
    "postgresql://postgres:Rohith1252@localhost:5432/ecommerce_source"
)


DATA_FOLDER = r"C:\Users\Rohith\ecommerce_pipeline\data"


files_to_load = {
    "olist_orders_dataset.csv"             : "raw_orders",
    "olist_customers_dataset.csv"          : "raw_customers",
    "olist_order_items_dataset.csv"        : "raw_order_items",
    "olist_products_dataset.csv"           : "raw_products",
    "olist_order_payments_dataset.csv"     : "raw_order_payments",
    "product_category_name_translation.csv": "raw_category_translation",
}


print("Loading CSV files into PostgreSQL...\n")

for filename, table_name in files_to_load.items():

    filepath = os.path.join(DATA_FOLDER, filename)

    if not os.path.exists(filepath):
        print(f"  WARNING: {filename} not found - skipping")
        continue

    print(f"Loading {filename}...")

    df = pd.read_csv(filepath)
    print(f"  Rows: {len(df):,}  Columns: {len(df.columns)}")

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )

    print(f"  Done - saved as table: {table_name}\n")


print("=" * 50)
print("All files loaded successfully!")
print("\nTables now in ecommerce_source database:")
for table_name in files_to_load.values():
    print(f"  - {table_name}")
print("=" * 50)


# ================================================================
# REFERENCE NOTES - read this to understand every part above
# ================================================================

# STEP 1 - IMPORTS
# ----------------------------------------------------------------
# import pandas as pd
#   pandas = library for working with tables of data in Python
#   a DataFrame = a table in memory, like Excel but in code
#   pd = short nickname everyone uses for pandas
#
# from sqlalchemy import create_engine
#   sqlalchemy = library that connects Python to databases
#   create_engine = opens a connection to PostgreSQL
#
# import os
#   built-in Python library for file and folder operations
#   we use it to build file paths and check if files exist


# STEP 2 - DATABASE CONNECTION
# ----------------------------------------------------------------
# create_engine("postgresql://postgres:password@localhost:5432/db")
#
#   postgresql://  = use PostgreSQL driver
#   postgres       = your PostgreSQL username
#   yourpassword   = your PostgreSQL password  ← CHANGE THIS
#   localhost      = database is on this computer
#   5432           = default PostgreSQL port
#   ecommerce_source = which database to connect to
#
#   think of it as a full address to your database
#   protocol://user:password@computer:door/room


# STEP 3 - DATA FOLDER
# ----------------------------------------------------------------
# DATA_FOLDER = r"C:\Users\Rohith\ecommerce_pipeline\data"
#
#   r"..." means raw string
#   backslashes in Windows paths need this
#   without the r, Python reads \n as newline, \t as tab etc
#   always use r"..." for Windows file paths


# STEP 4 - FILES TO LOAD DICTIONARY
# ----------------------------------------------------------------
# files_to_load = { "filename.csv" : "table_name" }
#
#   a dictionary maps keys to values
#   key   = CSV filename on your hard drive
#   value = table name to create in PostgreSQL
#
#   why a dictionary?
#   so we loop once and handle all 6 files with the same code
#   instead of writing loading logic 6 separate times
#
#   why prefix tables with raw_ ?
#   naming convention in data engineering
#   raw_ = untouched data exactly as it came from source
#   later transformed tables get different prefixes like stg_ or mart_


# STEP 5 - THE LOOP
# ----------------------------------------------------------------
# for filename, table_name in files_to_load.items():
#
#   .items() gives us both the key and value each iteration
#   first loop:  filename = "olist_orders_dataset.csv"
#                table_name = "raw_orders"
#   second loop: filename = "olist_customers_dataset.csv"
#                table_name = "raw_customers"
#   and so on for all 6 files


# STEP 6 - BUILD FILE PATH
# ----------------------------------------------------------------
# filepath = os.path.join(DATA_FOLDER, filename)
#
#   joins folder path + filename into one full path
#   result: C:\Users\Rohith\ecommerce_pipeline\data\olist_orders_dataset.csv
#
# if not os.path.exists(filepath):
#   checks if the file actually exists before reading
#   if missing we print a warning and skip with "continue"
#   continue = jump to next iteration of the loop immediately


# STEP 7 - READ CSV
# ----------------------------------------------------------------
# df = pd.read_csv(filepath)
#
#   reads the entire CSV file into a DataFrame in memory
#   df is the standard variable name for a DataFrame
#   len(df) = number of rows
#   len(df.columns) = number of columns
#
#   {len(df):,}
#   the :, formats numbers with commas  →  99441 shows as 99,441


# STEP 8 - WRITE TO POSTGRESQL
# ----------------------------------------------------------------
# df.to_sql(name, con, if_exists, index, chunksize)
#
#   name=table_name
#     name of table to create in the database
#
#   con=engine
#     the database connection we opened in STEP 2
#
#   if_exists="replace"
#     if this table already exists - drop it and recreate
#     makes the script safe to run multiple times
#     other options: "fail" = crash if exists
#                    "append" = add rows to existing table
#
#   index=False
#     pandas adds its own row numbers (0,1,2,3...)
#     we do not want those as a column in our database
#     index=False tells pandas to not write them
#
#   chunksize=1000
#     sends 1000 rows at a time to PostgreSQL
#     instead of all 100,000 at once
#     prevents memory errors on large files