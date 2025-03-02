import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import ace_tools_open as tools

from fixed_json_file import transaction

# Connexion à la base SQLite
db_name = "ecommerce.db"
conn = sqlite3.connect(db_name)

# Récupération des données
query_sales = '''
    SELECT p.product_name, SUM(s.quantity) as TotalQuantity, SUM(s.quantity * p.price) as TotalRevenue
    FROM sales s
    JOIN Products p ON s.product_id = p.product_id
    GROUP BY p.product_name
    ORDER BY TotalRevenue DESC;
'''
sales_data = pd.read_sql(query_sales, conn)

tools.display_dataframe_to_user(name="Revenue by product", dataframe=sales_data)

# Visualisation du chiffre d'affaires par produit
plt.figure(figsize=(15,7))
plt.bar(sales_data['product_name'], sales_data['TotalRevenue'], color='blue')
plt.xlabel("Products")
plt.ylabel("Revenue (€)")
plt.title("Revenue by product")
plt.xticks(rotation=45, ha='right')
plt.show()

# Récupération des commandes par client
query_orders = '''
    SELECT c.name, COUNT(t.transaction_id) as Total_Transaction
    FROM Transactions t
    JOIN Customers c ON t.customer_id = c.customer_id
    GROUP BY c.name
    ORDER BY Total_Transaction DESC;
'''
transaction_data = pd.read_sql(query_orders, conn)

tools.display_dataframe_to_user(name="Number of orders per customer", dataframe=transaction_data)

# Récupération des ventes par période (par mois)
query_monthly_sales = '''
    SELECT strftime('%Y-%m', transaction_date) as Month, SUM(s.quantity * p.price) as TotalRevenue
    FROM sales s
    JOIN Transactions t ON s.transaction_id = t.transaction_id
    JOIN Products p ON s.product_id = p.product_id
    GROUP BY Month
    ORDER BY Month;
'''
monthly_sales_data = pd.read_sql(query_monthly_sales, conn)

tools.display_dataframe_to_user(name="Revenue by Month", dataframe=monthly_sales_data)

# Visualisation des ventes par mois
plt.figure(figsize=(15,7))
plt.plot(monthly_sales_data['Month'], monthly_sales_data['TotalRevenue'], marker='o', linestyle='-', color='green')
plt.xlabel("Mois")
plt.ylabel("Revenue (€)")
plt.title("Évolution of Revenue by month")
plt.xticks(rotation=45, ha='right')
plt.grid()
plt.show()

# Fermeture de la connexion à la base de données
conn.close()

print("Analyze and reporting terminated with success !")
