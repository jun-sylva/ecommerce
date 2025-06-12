import sqlite3
import pandas as pd


def test_monthly_sales_query():
    conn = sqlite3.connect("ecommerce.db")
    query = """
        SELECT strftime('%Y-%m', transaction_date) as Month, SUM(s.quantity * p.price) as TotalRevenue
        FROM sales s
        JOIN Transactions t ON s.transaction_id = t.transaction_id
        JOIN Products p ON s.product_id = p.product_id
        GROUP BY Month
        ORDER BY Month;
    """
    df = pd.read_sql(query, conn)
    assert not df.empty
    conn.close()
