import sqlite3

# Connexion on db or create if not exist
conn = sqlite3.connect("ecommerce.db")
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
        sale_id BIGINT PRIMARY KEY,
        transaction_id TEXT,
        product_id TEXT,
        quantity BIGINT NOT NULL,
        total_amount FLOAT NOT NULL,
        FOREIGN KEY (transaction_id) REFERENCES Transactions(transaction_id),
        FOREIGN KEY (product_id) REFERENCES Products(product_id)
    );
''')

print("Tables créées avec succès !")

# Check list of tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# Show tables
print("Tables dans la base de données :")
for table in tables:
    print(table[0])

