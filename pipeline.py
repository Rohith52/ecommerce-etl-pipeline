from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(__file__))
from load.load_to_warehouse import run_load


def run_pipeline():
    print("\n" + "=" * 50)
    print(f"PIPELINE STARTED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    try:
        run_load()
        print("\n" + "=" * 50)
        print(f"PIPELINE COMPLETED SUCCESSFULLY")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)

    except Exception as e:
        print("\n" + "=" * 50)
        print(f"PIPELINE FAILED: {str(e)}")
        print(f"Failed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)


def main():
    print("=" * 50)
    print("ECOMMERCE ETL PIPELINE SCHEDULER")
    print("=" * 50)

    # run immediately once when script starts
    print("\nRunning pipeline immediately on startup...")
    run_pipeline()

    # then schedule to run every day at midnight
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline,
        trigger="cron",
        hour=0,
        minute=0,
        id="ecommerce_etl"
    )

    print("\nScheduler started - pipeline will run daily at midnight")
    print("Press Ctrl+C to stop\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\nScheduler stopped by user")
        scheduler.shutdown()


if __name__ == "__main__":
    main()


# ================================================================
# REFERENCE NOTES
# ================================================================

# WHAT THIS SCRIPT DOES
# ----------------------------------------------------------------
# This is the master script - the entry point of the pipeline.
# It does two things:
#   1. Runs the full pipeline immediately when you start it
#   2. Schedules it to run automatically every day at midnight
#
# In production this would be Airflow.
# APScheduler is the lightweight local equivalent.
# Same concept - a scheduler that triggers your pipeline
# on a defined schedule without you doing anything manually.


# APSCHEDULER
# ----------------------------------------------------------------
# APScheduler = Advanced Python Scheduler
# A library that runs Python functions on a schedule.
# Works natively on Windows - no Linux required.
#
# BlockingScheduler
#   runs in the foreground - keeps the script alive
#   script stays running and waits for next scheduled time
#   Ctrl+C stops it cleanly
#
# scheduler.add_job()
#   func = which function to run (run_pipeline)
#   trigger = "cron" means run on a time schedule
#             like a cron job in Linux
#   hour=0, minute=0 = run at 00:00 = midnight every day


# TRY / EXCEPT - ERROR HANDLING
# ----------------------------------------------------------------
# try:
#     run_load()       ← attempt this
# except Exception as e:
#     print(e)         ← if anything goes wrong, catch it
#
# Without try/except - if anything fails the whole script
# crashes and the scheduler stops forever.
# With try/except - if one pipeline run fails, we log the
# error and the scheduler keeps running for the next day.
# This is critical for production pipelines.
# A pipeline that stops scheduling on first error is useless.


# HOW THE CHAIN WORKS
# ----------------------------------------------------------------
# pipeline.py
#   calls run_load()
#     calls run_transform()
#       calls run_extract()
#         reads from ecommerce_source
#       cleans and builds 8 tables
#     returns 8 DataFrames
#   writes all 8 tables to ecommerce_warehouse
#
# One script. One function call. Entire pipeline runs.


# SCHEDULING IN PRODUCTION VS LOCAL
# ----------------------------------------------------------------
# Local (what we built):
#   APScheduler inside a Python script
#   runs as long as your computer is on
#   good enough for portfolio and learning
#
# Production options:
#   Apache Airflow  - industry standard, full UI, retries,
#                     dependencies, monitoring. Runs on Linux.
#   AWS EventBridge - trigger pipelines on AWS cloud
#   Google Cloud Scheduler - same but on GCP
#   Prefect / Dagster - modern Airflow alternatives
#
# The scheduling CONCEPT is identical across all of these.
# You define: what to run, when to run it, what to do on failure.
# Only the tool changes.
