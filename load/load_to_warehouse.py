import pandas as pd
from sqlalchemy import create_engine
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from transform.transform_orders import run_transform


warehouse_engine = create_engine(
    "postgresql://postgres:Rohith1252@localhost:5432/ecommerce_warehouse"
)


def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(
        name=table_name,
        con=warehouse_engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )
    print(f"  Done - {len(df):,} rows loaded")


def run_load():
    print("=" * 50)
    print("LOAD STAGE STARTED")
    print("=" * 50)

    transformed = run_transform()

    print("\nLoading tables into warehouse...")
    for table_name, df in transformed.items():
        load_table(df, table_name)

    print("\nLOAD COMPLETE")
    print("\nTables now in ecommerce_warehouse:")
    for table_name in transformed.keys():
        print(f"  - {table_name}")


if __name__ == "__main__":
    run_load()


# ================================================================
# REFERENCE NOTES
# ================================================================

# WHAT THIS SCRIPT DOES
# ----------------------------------------------------------------
# This is the final stage of the ETL pipeline.
# It takes the 4 clean transformed tables and writes them
# permanently into the warehouse database.
#
# Before this step the transformed tables only existed in
# Python memory - they disappeared when the script ended.
# After this step they live in PostgreSQL forever and
# anything can connect to them - Tableau, SQL queries,
# other pipelines, dashboards, analysts.


# WHY A SEPARATE WAREHOUSE DATABASE
# ----------------------------------------------------------------
# We write to ecommerce_warehouse not ecommerce_source
#
# ecommerce_source = raw untouched data, the "source of truth"
#                    never modify this
#
# ecommerce_warehouse = clean transformed data for analysis
#                       this is what analysts and dashboards use
#
# keeping them separate means:
#   analysts can run heavy queries on warehouse without
#   slowing down the source database
#   if pipeline breaks warehouse still has last good data
#   clear separation of raw vs clean data


# HOW IT CONNECTS TO THE OTHER SCRIPTS
# ----------------------------------------------------------------
# run_load()
#   calls run_transform()
#     which calls run_extract()
#       which reads from ecommerce_source
#     which cleans and joins everything
#   which returns 4 clean DataFrames
# then writes each DataFrame to ecommerce_warehouse
#
# the full chain:
# load → transform → extract → source database
# one function call triggers the entire pipeline


# load_table() FUNCTION
# ----------------------------------------------------------------
# takes a DataFrame and a table name
# writes it to the warehouse database
# if_exists="replace" means safe to run multiple times
# each run overwrites with fresh transformed data
# this is called a "full refresh" pattern
# every pipeline run replaces all warehouse data from scratch