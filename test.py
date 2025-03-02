import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import ace_tools_open as tools

# Connexion à la base SQLite
db_name = "ecommerce.db"
conn = sqlite3.connect(db_name)

# Récupération des données
query_monthly_sales = '''
    SELECT strftime('%Y-%m', transaction_date) as Month, SUM(s.quantity * p.price) as TotalRevenue
    FROM sales s
    JOIN Transactions t ON s.transaction_id = t.transaction_id
    JOIN Products p ON s.product_id = p.product_id
    GROUP BY Month
    ORDER BY Month;
'''
monthly_sales_data = pd.read_sql(query_monthly_sales, conn)

tools.display_dataframe_to_user(name="Chiffre d'affaires par mois", dataframe=monthly_sales_data)
conn.close()