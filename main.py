from data_ingestion import *
import numpy as np
import os

# Use a path relative to this file to improve portability
BASE_DIR = os.path.dirname(__file__)
path = os.path.join(BASE_DIR, "transactions_fixed.json")
db_name = os.path.join(BASE_DIR, 'ecommerce.db')

if __name__ == "__main__":
    conn = conn_sqlite(db_name)
    transaction = load_json(path)

    # Create Customers dataframe
    name_cust = "Customers"
    customer_df = transaction["customer"].apply(pd.Series)

    ## Clean Customers dataframe
    customer_df = data_cleaning(customer_df, name_cust)

    ## Load Customers dataframe on Sqlite
    save_on_sqlite(customer_df, name_cust, conn)

    # Create Products dataframe
    name_prd = "Products"
    transaction["customer_id"] = transaction["customer"].apply(lambda x: x["customer_id"])
    df_exploded = transaction.explode("shopping_cart", ignore_index=True)
    products_df = df_exploded["shopping_cart"].apply(pd.Series)
    products_df = pd.concat([df_exploded[["transaction_id", "customer_id"]], products_df], axis=1)
    products_df = products_df[['product_id', 'product_name', 'product_category', 'price', 'vendor']]

    ## Clean Products dataframe
    products_df = data_cleaning(products_df, name_prd)

    ## Load Products dataframe on Sqlite
    save_on_sqlite(products_df, name_prd, conn)

    # Create Sales dataframe
    name_sls = 'sales'
    sale_df = df_exploded["shopping_cart"].apply(pd.Series)
    sale_df['sale_id'] = np.random.randint(10 ** 7, 10 ** 8, size=len(sale_df))
    sale_df = pd.concat([df_exploded[["transaction_id", "total_amount"]], sale_df], axis=1)

    ## Clean Sales dataframe
    sale_df = sale_df[['sale_id', 'transaction_id', 'product_id', 'quantity', 'total_amount']]
    sale_df = data_cleaning(sale_df, name_sls)

    ## Load Products dataframe on Sqlite
    save_on_sqlite(sale_df, name_sls, conn)

    # Clean Transactions dataframe
    name_trs = "Transactions"
    transaction = transaction[['transaction_id', 'customer_id', 'shipping_method','transaction_date']]
    transaction = data_cleaning(transaction, name_trs)

    # Load Transaction dataframe on Sqlite
    save_on_sqlite(transaction, name_trs, conn)

    conn.commit()
    conn.close()


