import pandas as pd
import sqlite3
import logging

logging.basicConfig(
    filename="ecommerce.log",
    level=logging.DEBUG,
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

# Connexion à SQLite
def conn_sqlite(db_name):
    try:
        conn = sqlite3.connect(db_name)
        logging.info("Connected!!")
        print("Connected!!")
        return conn
    except Exception as e:
        logging.error(f"Error to connect on: {e}")


def load_json(file_path):
    try:
        df = pd.read_json(file_path)
        logging.info(f"File is opened with success")
        print(f"File is opened with success")
        return df
    except Exception as e:
        logging.error(f"failed to open: {e}")


def data_cleaning(df, name):
    try:
        # Drop duplicate values
        df.drop_duplicates(inplace=True)

        # Drop row with blank values
        df.dropna(inplace=True)

        # Normalizing column names
        df.columns = df.columns.str.lower()
        logging.info(f"dataframe {name} is cleaned!!")
        print(f"dataframe {name} is cleaned!!")
        return df
    except Exception as e:
        logging.error(f"Error to clean: {e}")


def save_on_sqlite(df, name, conn):
    try:
        # Connexion on db or create if not exist
        cursor = conn.cursor()

        # Create Table
        cursor.executescript('''
                CREATE TABLE IF NOT EXISTS Customers (
                    customer_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    phone TEXT,
                    address TEXT
                );

                CREATE TABLE IF NOT EXISTS Products (
                    product_id TEXT PRIMARY KEY,
                    product_name TEXT NOT NULL,
                    product_category TEXT NOT NULL,
                    price FLOAT NOT NULL,
                    vendor TEXT
                );

                CREATE TABLE IF NOT EXISTS Transactions (
                    transaction_id TEXT PRIMARY KEY,
                    customer_id TEXT,
                    shipping_method TEXT,
                    transaction_date TEXT NOT NULL,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                );

                CREATE TABLE IF NOT EXISTS sales (
                    sale_id INTEGER PRIMARY KEY,
                    transaction_id TEXT,
                    product_id TEXT,
                    quantity INTEGER NOT NULL,
                    total_amount FLOAT NOT NULL,
                    FOREIGN KEY (transaction_id) REFERENCES Transactions(transaction_id),
                    FOREIGN KEY (product_id) REFERENCES Products(product_id)
                );
            ''')

        df.to_sql(name, conn, if_exists="replace", index=False)
        logging.info(f"{name} Data is loaded on Sqlite with success!!")
        print(f"{name} Data is loaded on Sqlite with success!!")
    except Exception as e:
        logging.error(f"failed to load {name}: {e}")
