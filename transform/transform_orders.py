import pandas as pd
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from extract.extract_orders import run_extract


def transform(data):

    orders      = data["orders"].copy()
    customers   = data["customers"].copy()
    order_items = data["order_items"].copy()
    products    = data["products"].copy()
    payments    = data["payments"].copy()


    # ----------------------------------------------------------
    # CLEAN ORDERS
    # ----------------------------------------------------------

    # convert timestamp columns from text to proper datetime
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in timestamp_cols:
        orders[col] = pd.to_datetime(orders[col], errors="coerce")
    # errors="coerce" means: if a value cannot be converted
    # to a datetime, replace it with NaT (Not a Time) instead
    # of crashing the script. Real data always has bad dates.

    # keep only delivered orders for our analysis
    # cancelled and unavailable orders skew revenue numbers
    orders = orders[orders["order_status"] == "delivered"]
    print(f"  Orders after filtering to delivered: {len(orders):,}")

    # add a simple order date column (date only, no time)
    orders["order_date"] = orders["order_purchase_timestamp"].dt.date


    # ----------------------------------------------------------
    # BUILD MASTER TABLE
    # join all tables together into one wide table
    # this is the foundation every aggregation is built on
    # ----------------------------------------------------------

    # step 1: orders + customers
    master = orders.merge(
        customers,
        on="customer_id",
        how="left"
    )
    # merge = SQL JOIN in pandas
    # on="customer_id" = the column to join on
    # how="left" = keep all orders even if no customer match

    # step 2: master + order_items
    master = master.merge(
        order_items,
        on="order_id",
        how="left"
    )

    # step 3: master + products
    master = master.merge(
        products,
        on="product_id",
        how="left"
    )

    # step 4: master + payments
    payments_agg = payments.groupby("order_id").agg(
        payment_type=("payment_type", "first"),
        payment_installments=("payment_installments", "max"),
        payment_value=("payment_value", "sum")
    ).reset_index()
    # payments has multiple rows per order (installments)
    # we aggregate to one row per order before joining
    # first payment type, max installments, total value

    master = master.merge(
        payments_agg,
        on="order_id",
        how="left"
    )

    # calculate revenue per line item
    master["revenue"] = master["price"] + master["freight_value"]

    print(f"  Master table built: {len(master):,} rows")


    # ----------------------------------------------------------
    # BUILD ANALYTICAL TABLES
    # these are the final tables that go into the warehouse
    # each one answers a specific business question
    # ----------------------------------------------------------

    # TABLE 1: daily revenue
    # answers: what was our revenue each day?
    daily_revenue = master.groupby("order_date").agg(
        total_orders=("order_id", "nunique"),
        total_items=("order_item_id", "count"),
        total_revenue=("revenue", "sum"),
        avg_order_value=("payment_value", "mean")
    ).reset_index()
    # groupby = group rows by date
    # agg = apply different aggregations to different columns
    # nunique = count unique values (unique orders per day)
    # reset_index = turn the groupby result back into a flat table

    daily_revenue["total_revenue"] = daily_revenue["total_revenue"].round(2)
    daily_revenue["avg_order_value"] = daily_revenue["avg_order_value"].round(2)
    daily_revenue = daily_revenue.sort_values("order_date")
    print(f"  daily_revenue: {len(daily_revenue):,} rows")


    # TABLE 2: top products
    # answers: which products and categories sell the most?
    top_products = master.groupby(
        ["product_id", "category_english"]
    ).agg(
        total_items_sold=("order_item_id", "count"),
        total_revenue=("revenue", "sum"),
        avg_price=("price", "mean")
    ).reset_index()

    top_products["total_revenue"] = top_products["total_revenue"].round(2)
    top_products["avg_price"] = top_products["avg_price"].round(2)
    top_products = top_products.sort_values(
        "total_revenue", ascending=False
    )
    print(f"  top_products: {len(top_products):,} rows")


    # TABLE 3: customer summary
    # answers: who are our best customers?
    customer_summary = master.groupby(
        ["customer_unique_id", "customer_city", "customer_state"]
    ).agg(
        total_orders=("order_id", "nunique"),
        total_spent=("payment_value", "sum"),
        first_order=("order_date", "min"),
        last_order=("order_date", "max")
    ).reset_index()

    customer_summary["total_spent"] = customer_summary["total_spent"].round(2)
    customer_summary = customer_summary.sort_values(
        "total_spent", ascending=False
    )
    print(f"  customer_summary: {len(customer_summary):,} rows")


    # TABLE 4: delivery performance
    # answers: how fast do we deliver by state?
    orders["delivery_days"] = (
        orders["order_delivered_customer_date"] -
        orders["order_purchase_timestamp"]
    ).dt.days
    # subtract two datetime columns = timedelta
    # .dt.days converts timedelta to number of days

    delivery = orders.merge(customers, on="customer_id", how="left")
    delivery_performance = delivery.groupby("customer_state").agg(
        total_orders=("order_id", "count"),
        avg_delivery_days=("delivery_days", "mean"),
        min_delivery_days=("delivery_days", "min"),
        max_delivery_days=("delivery_days", "max")
    ).reset_index()

    delivery_performance["avg_delivery_days"] = (
        delivery_performance["avg_delivery_days"].round(1)
    )
    delivery_performance = delivery_performance.sort_values(
        "avg_delivery_days"
    )
    print(f"  delivery_performance: {len(delivery_performance):,} rows")


    # transformed = {
    #     "daily_revenue"       : daily_revenue,
    #     "top_products"        : top_products,
    #     "customer_summary"    : customer_summary,
    #     "delivery_performance": delivery_performance,
    # }replacing this with below star scheme...fact and dimension

    # ----------------------------------------------------------
    # STAR SCHEMA TABLES
    # ----------------------------------------------------------

    # DIMENSION 1: dim_customers
    # one row per unique customer
    # descriptive info only - no numbers, no aggregations
    dim_customers = customers[[
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix"
    ]].drop_duplicates(subset="customer_unique_id")
    # drop_duplicates removes duplicate rows
    # subset="customer_unique_id" means check only that column
    # same customer can appear multiple times in orders
    # dimension tables need one row per entity
    print(f"  dim_customers: {len(dim_customers):,} rows")


    # DIMENSION 2: dim_products
    # one row per unique product with English category name
    dim_products = products[[
        "product_id",
        "category_english",
    ]].drop_duplicates(subset="product_id")
    print(f"  dim_products: {len(dim_products):,} rows")


    # DIMENSION 3: dim_date
    # one row per unique date with time breakdowns
    # this is the date dimension - every warehouse has one
    # it lets analysts filter and group by month, quarter,
    # year without doing date math in every query
    all_dates = pd.date_range(
        start=orders["order_purchase_timestamp"].min(),
        end=orders["order_purchase_timestamp"].max(),
        freq="D"
    )
    # pd.date_range generates every date between start and end
    # freq="D" means daily frequency

    dim_date = pd.DataFrame({
        "date"      : all_dates.date,
        "day"       : all_dates.day,
        "month"     : all_dates.month,
        "month_name": all_dates.strftime("%B"),
        "quarter"   : all_dates.quarter,
        "year"      : all_dates.year,
        "day_of_week"    : all_dates.dayofweek,
        "day_name"       : all_dates.strftime("%A"),
        "is_weekend"     : all_dates.dayofweek >= 5,
    })
    print(f"  dim_date: {len(dim_date):,} rows")


    # FACT TABLE: fact_order_items
    # one row per item sold - the grain of this fact table
    # "grain" means what exactly does one row represent
    # here: one item in one order
    # contains foreign keys to all dimensions + measures
    #
    # foreign keys = IDs that link to dimension tables
    # measures = the actual numbers we want to analyze
    #            (price, freight, revenue)

    fact_order_items = order_items.merge(
        orders[["order_id", "customer_id",
                "order_purchase_timestamp", "order_date",
                "order_status"]],
        on="order_id",
        how="left"
    )

    fact_order_items = fact_order_items.merge(
        products[["product_id", "category_english"]],
        on="product_id",
        how="left"
    )

    # calculate revenue per line item
    fact_order_items["revenue"] = (
        fact_order_items["price"] +
        fact_order_items["freight_value"]
    )

    # keep only the columns we need in the fact table
    fact_order_items = fact_order_items[[
        "order_item_id",
        "order_id",
        "product_id",
        "seller_id",
        "customer_id",
        "order_date",
        "category_english",
        "price",
        "freight_value",
        "revenue",
        "order_status",
    ]]

    print(f"  fact_order_items: {len(fact_order_items):,} rows")


    # return ALL tables - both aggregated marts and star schema
    transformed = {
        # aggregated mart tables (pre-computed answers)
        "daily_revenue"       : daily_revenue,
        "top_products"        : top_products,
        "customer_summary"    : customer_summary,
        "delivery_performance": delivery_performance,
        # star schema tables
        "fact_order_items"    : fact_order_items,
        "dim_customers"       : dim_customers,
        "dim_products"        : dim_products,
        "dim_date"            : dim_date,
    }

    return transformed



def run_transform():
    print("=" * 50)
    print("TRANSFORM STAGE STARTED")
    print("=" * 50)

    print("Running extract first...")
    data = run_extract()

    print("\nTransforming data...")
    transformed = transform(data)

    print("\nTRANSFORM COMPLETE")
    print("Tables ready for loading:")
    for name, df in transformed.items():
        print(f"  {name}: {len(df):,} rows")

    return transformed


if __name__ == "__main__":
    transformed = run_transform()
    print("\nSample daily_revenue:")
    print(transformed["daily_revenue"].head(5))
    print("\nSample top_products:")
    print(transformed["top_products"].head(5))


# ================================================================
# REFERENCE NOTES
# ================================================================

# STEP 1 - IMPORTS AND sys.path
# ----------------------------------------------------------------
# sys.path.append(...)
#   Python needs to know where to find your extract script
#   sys.path is the list of folders Python searches for modules
#   we add the project root folder to that list
#   then "from extract.extract_orders import run_extract" works


# STEP 2 - .copy()
# ----------------------------------------------------------------
# orders = data["orders"].copy()
#
#   always copy DataFrames before modifying them
#   without .copy() you modify the original data in memory
#   with .copy() you work on a separate copy
#   prevents hard to find bugs where data changes unexpectedly


# STEP 3 - pd.to_datetime()
# ----------------------------------------------------------------
# converts text columns like "2017-10-10 21:25:13"
# into proper datetime objects Python can do math on
# errors="coerce" replaces bad values with NaT
# NaT = Not a Time, the datetime version of NaN/NULL


# STEP 4 - merge() = JOIN
# ----------------------------------------------------------------
# df1.merge(df2, on="column", how="left")
#
#   this is a SQL JOIN but in pandas
#   on="column"  = the column both tables share
#   how="left"   = keep all rows from left table
#   how="inner"  = keep only matching rows
#   how="outer"  = keep all rows from both tables


# STEP 5 - groupby().agg()
# ----------------------------------------------------------------
# master.groupby("order_date").agg(
#     total_orders=("order_id", "nunique"),
#     total_revenue=("revenue", "sum")
# )
#
#   groupby = group all rows that share the same date
#   agg = apply aggregation functions to each group
#   ("order_id", "nunique") = count unique order IDs
#   ("revenue", "sum")      = sum all revenue values
#   result = one row per date with aggregated columns
#
#   this is exactly like SQL:
#   SELECT order_date,
#          COUNT(DISTINCT order_id) as total_orders,
#          SUM(revenue) as total_revenue
#   FROM master
#   GROUP BY order_date


# STEP 6 - reset_index()
# ----------------------------------------------------------------
# after groupby the grouped column becomes the index
# reset_index() turns it back into a normal column
# always call this after groupby().agg()


# STEP 7 - dt.days
# ----------------------------------------------------------------
# orders["delivered"] - orders["purchased"] = timedelta
# a timedelta is a duration - like "5 days 3 hours"
# .dt.days extracts just the days part as a number
# this gives us delivery time in days per order
