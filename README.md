# Ecommerce Data Pipeline

This repository contains a simple ETL pipeline for processing sample e-commerce transaction data. Raw JSON files are cleaned and loaded into a SQLite database before generating basic reports. An Airflow DAG is also provided to schedule the workflow.

## Project Structure

- **data_ingestion.py** – Helper functions for connecting to SQLite, loading JSON data with pandas, cleaning DataFrames and saving tables.
- **main.py** – Main ETL script that parses `transactions_fixed.json`, creates tables for customers, products, sales and transactions, and stores them into `ecommerce.db`.
- **fixed_json_file.py** – Utility script used once to fix inconsistent keys in the original `transactions.json` file and produce `transactions_fixed.json`.
- **analyze_and_reporting.py** – Reads aggregated data from SQLite and displays simple revenue and transaction summaries using matplotlib.
- **airflow.py** – Defines an Airflow DAG named `etl_pipeline` executing the extract, transform and load steps daily.
- **sqlite.py** – Creates the SQLite schema manually and lists all tables for quick inspection.
- **test.py** – Small test snippet querying monthly revenue from the database.
- **transactions.json** – Original dataset of e-commerce transactions.
- **transactions_fixed.json** – Corrected dataset used for ingestion.
- **ecommerce.db** – SQLite database populated by the pipeline.

## Quick Start

1. Install Python 3 with required packages such as `pandas`, `numpy`, `matplotlib`, `sqlite3`, and (optionally) `apache-airflow`.
2. Update the file paths in the scripts if necessary, then run the ETL process:

   ```bash
   python main.py
   ```

3. After running `main.py`, `ecommerce.db` will contain four tables: `Customers`, `Products`, `Transactions`, and `sales`.
4. For analysis, execute:

   ```bash
   python analyze_and_reporting.py
   ```

   This will generate simple charts of revenue by product and by month.
5. To automate the pipeline, integrate `airflow.py` with an existing Airflow installation.

## Notes

The dataset and database included in this repository are small samples intended for demonstration only. The scripts can be adapted to larger datasets or other storage engines as needed.
