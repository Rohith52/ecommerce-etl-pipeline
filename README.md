# Ecommerce ETL Pipeline

An end-to-end data engineering pipeline built on real Brazilian e-commerce data from Olist. Automatically extracts, transforms, and loads 100,000+ transactions into a structured data warehouse with both aggregated analytical tables and a star schema design.

---

## What This Project Does

Raw e-commerce data arrives across multiple tables — orders, customers, products, payments. This pipeline automatically:

1. Extracts raw data from a PostgreSQL source database
2. Cleans and transforms it — handling nulls, fixing timestamps, joining tables
3. Loads 8 production-ready tables into a warehouse database
4. Runs on a daily schedule automatically

---

## Architecture
```
Raw CSV Files (Olist Dataset)
        ↓
PostgreSQL Source Database
  raw_orders · raw_customers · raw_order_items
  raw_products · raw_order_payments
        ↓
ETL Pipeline (Python)
  Extract → Transform → Load
        ↓
PostgreSQL Warehouse Database
  Aggregated Marts    │  Star Schema
  ─────────────────   │  ───────────────
  daily_revenue       │  fact_order_items
  top_products        │  dim_customers
  customer_summary    │  dim_products
  delivery_performance│  dim_date
        ↓
Tableau Dashboard
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python | Pipeline logic |
| PostgreSQL | Source and warehouse database |
| pandas | Data transformation |
| SQLAlchemy | Database connectivity |
| APScheduler | Pipeline scheduling |
| Tableau | Data visualization |

---

## Project Structure
```
ecommerce_pipeline/
├── extract/
│   └── extract_orders.py      # pulls raw data from source DB
├── transform/
│   └── transform_orders.py    # cleans, joins, aggregates
├── load/
│   └── load_to_warehouse.py   # writes to warehouse DB
├── data/
│   └── load_source_data.py    # one-time CSV loader
└── pipeline.py                # master scheduler
```

---

## Warehouse Design

### Aggregated Mart Tables
Pre-computed answers to common business questions:
- `daily_revenue` — total orders, items, revenue and average order value per day across 612 days
- `top_products` — products ranked by revenue with category breakdown
- `customer_summary` — lifetime value, order count, first and last order per customer
- `delivery_performance` — average delivery days across 27 Brazilian states

### Star Schema
Dimensional model for flexible ad-hoc analysis:
- `fact_order_items` — 112,650 rows, one per item sold, with price, freight and revenue
- `dim_customers` — 96,096 unique customers with city and state
- `dim_products` — 32,951 products with English category names
- `dim_date` — 714 days with day, month, quarter, year and weekend flag

---

## Dataset

**Brazilian E-Commerce Public Dataset by Olist**
- Source: Kaggle
- 99,441 orders · 100,000+ customers · 112,650 order items
- Period: 2016 to 2018
- 8 relational tables mirroring a real production database

---

## How To Run

**1 — Clone the repo**
```
git clone https://github.com/Rohith52/ecommerce-etl-pipeline.git
```

**2 — Install dependencies**
```
pip install pandas sqlalchemy psycopg2-binary apscheduler
```

**3 — Set up PostgreSQL databases**
```sql
CREATE DATABASE ecommerce_source;
CREATE DATABASE ecommerce_warehouse;
```

**4 — Download Olist dataset**

Download from Kaggle and place CSV files in the `data/` folder.

**5 — Load source data**
```
python data/load_source_data.py
```

**6 — Run the pipeline**
```
python pipeline.py
```

Pipeline runs immediately then schedules daily at midnight.

---

## Key Engineering Decisions

**Why separate source and warehouse databases?**
Analysts run heavy aggregation queries without touching the operational source. If the pipeline breaks the warehouse retains the last successful load.

**Why both mart tables and star schema?**
Mart tables give analysts pre-computed fast answers. The star schema enables flexible ad-hoc analysis with any combination of dimensions. Real production warehouses use both.

**Why APScheduler instead of Airflow?**
Airflow does not run natively on Windows. APScheduler delivers identical scheduling behaviour locally. In production this pipeline would be orchestrated by Airflow on a Linux server or a managed cloud scheduler.

**Why full refresh instead of incremental?**
The Olist dataset is static. In production with live data, incremental loading would extract only rows newer than the last run timestamp — significantly reducing compute and load time.
