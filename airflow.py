from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_ingestion import *
import pandas as pd
import numpy as np
import logging

logging.basicConfig(
    filename="ecommerce_airflow.log",
    level=logging.DEBUG,
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

# Configuration DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL pour ingestion et transformation des données",
    schedule_interval=timedelta(days=1),
)

import os

# Paths relative to this file for portability
BASE_DIR = os.path.dirname(__file__)
db_name = os.path.join(BASE_DIR, 'ecommerce.db')
path = os.path.join(BASE_DIR, 'transactions_fixed.json')
name_cust = "Customers"
name_prd = "Products"
name_sls = 'sales'
name_trs = "Transactions"

def extract_data():
    """ Extract JSON file """
    transaction = load_json(path)
    return transaction


def transform_data():
    """ Clean and transform data """
    transaction = extract_data()
    #Customers
    customers_df = transaction["customer"].apply(pd.Series)
    customers_df = data_cleaning(customers_df, name_cust)
    # Products
    transaction["customer_id"] = transaction["customer"].apply(lambda x: x["customer_id"])
    df_exploded = transaction.explode("shopping_cart", ignore_index=True)
    products_df = df_exploded["shopping_cart"].apply(pd.Series)
    products_df = pd.concat([df_exploded[["transaction_id", "customer_id"]], products_df], axis=1)
    products_df = products_df[['product_id', 'product_name', 'product_category', 'price', 'vendor']]
    products_df = data_cleaning(products_df, name_prd)
    # Sales
    sales_df = df_exploded["shopping_cart"].apply(pd.Series)
    sales_df['sale_id'] = np.random.randint(10 ** 7, 10 ** 8, size=len(sales_df))
    sales_df = pd.concat([df_exploded[["transaction_id", "total_amount"]], sales_df], axis=1)
    sales_df = sales_df[['sale_id', 'transaction_id', 'product_id', 'quantity', 'total_amount']]
    sales_df = data_cleaning(sales_df, name_sls)
    # Transactions
    transactions_df = transaction[['transaction_id', 'customer_id', 'shipping_method', 'transaction_date']]
    transactions_df = data_cleaning(transactions_df, name_trs)
    return customers_df, products_df, sales_df, transactions_df


def load_data():
    """ Load data transform in SQLite """
    conn = conn_sqlite(db_name)
    customers_df, products_df, sales_df, transactions_df = transform_data()

    save_on_sqlite(customers_df, name_cust, conn)
    save_on_sqlite(products_df, name_prd, conn)
    save_on_sqlite(sales_df, name_sls, conn)
    save_on_sqlite(transactions_df, name_trs, conn)

    conn.commit()
    conn.close()


extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

extract_task >> transform_task >> load_task
