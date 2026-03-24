import pandas as pd
from sqlalchemy import create_engine


engine = create_engine(
    "postgresql://postgres:Rohith1252@localhost:5432/ecommerce_source"
)


def extract_orders():
    print("Extracting orders...")
    df = pd.read_sql("SELECT * FROM raw_orders", engine)
    print(f"  Got {len(df):,} rows")
    return df

def extract_customers():
    print("Extracting customers...")
    df = pd.read_sql("SELECT * FROM raw_customers", engine)
    print(f"  Got {len(df):,} rows")
    return df

def extract_order_items():
    print("Extracting order items...")
    df = pd.read_sql("SELECT * FROM raw_order_items", engine)
    print(f"  Got {len(df):,} rows")
    return df

def extract_products():
    print("Extracting products...")
    df = pd.read_sql("""
        SELECT
            p.product_id,
            p.product_category_name,
            COALESCE(t.product_category_name_english,
                     p.product_category_name) AS category_english
        FROM raw_products p
        LEFT JOIN raw_category_translation t
            ON p.product_category_name = t.product_category_name
    """, engine)
    print(f"  Got {len(df):,} rows")
    return df

def extract_payments():
    print("Extracting payments...")
    df = pd.read_sql("SELECT * FROM raw_order_payments", engine)
    print(f"  Got {len(df):,} rows")
    return df


def run_extract():
    print("=" * 50)
    print("EXTRACT STAGE STARTED")
    print("=" * 50)

    data = {
        "orders"      : extract_orders(),
        "customers"   : extract_customers(),
        "order_items" : extract_order_items(),
        "products"    : extract_products(),
        "payments"    : extract_payments(),
    }

    print("\nEXTRACT COMPLETE")
    return data


if __name__ == "__main__":
    data = run_extract()
    print("\nSample orders:")
    print(data["orders"].head(3))


# ================================================================
# REFERENCE NOTES
# ================================================================

# STEP 1 - WHY SEPARATE EXTRACT SCRIPT
# ----------------------------------------------------------------
# Single responsibility principle.
# This script does ONE thing only - read data from source.
# It does not clean, does not transform, does not load.
# Benefits:
#   if transform breaks  → we do not re-extract
#   if source changes    → we only fix this one file
#   easy to test alone   → run just this file and check output


# STEP 2 - pd.read_sql()
# ----------------------------------------------------------------
# pd.read_sql("SELECT * FROM table", engine)
#
#   runs the SQL query against your database
#   returns the result as a pandas DataFrame
#   engine = the connection we opened at the top
#
#   you write normal SQL - pandas handles the rest
#   no manual looping through rows needed


# STEP 3 - ONE FUNCTION PER TABLE
# ----------------------------------------------------------------
# extract_orders(), extract_customers() etc
#
#   each table gets its own function
#   why? if orders extraction fails we know exactly where
#   clean separation = easy debugging
#   each function returns one DataFrame


# STEP 4 - COALESCE IN extract_products
# ----------------------------------------------------------------
# COALESCE(value1, value2)
#
#   returns the first non-null value
#   our products table has category names in Portuguese
#   we join the translation table to get English names
#   if no translation exists COALESCE falls back to
#   the Portuguese name instead of showing NULL
#
#   LEFT JOIN means keep all products even if no translation


# STEP 5 - run_extract() MASTER FUNCTION
# ----------------------------------------------------------------
# returns a dictionary of DataFrames
#
#   data["orders"]      = orders DataFrame
#   data["customers"]   = customers DataFrame
#   data["order_items"] = order items DataFrame
#
#   why a dictionary?
#   the transform script calls run_extract() and gets
#   all tables in one clean object
#   no global variables no files no extra database calls


# STEP 6 - if __name__ == "__main__"
# ----------------------------------------------------------------
# when you run:  python extract\extract_orders.py
#   __name__ = "__main__"  → the test block runs
#
# when transform imports this file:
#   __name__ = module name → test block is SKIPPED
#
# this means you can test this file alone AND import it
# from other scripts without running anything twice