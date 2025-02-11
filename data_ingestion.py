import pandas as pd
import sqlite3

# Connexion à SQLite
def conn_sqlite(db_name):
    try:
        conn = sqlite3.connect(db_name)
        print("Connected!!")
        return conn
    except Exception as e:
        print(f"Error to connect on: {e}")


def load_json(file_path):
    return pd.read_json(file_path)

def data_cleaning(df):
    # Drop duplicate values
    df.drop_duplicates(inplace=True)

    # Drop row with blank values
    df.dropna(inplace=True)

    # Normalizing column names
    df.columns = df.columns.str.lower()

    return df

def save_on_sqlite(df, name, conn):
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
    print(f"{name} Data is loaded on Sqlite with success!!")



