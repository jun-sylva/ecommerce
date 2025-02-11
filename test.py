import pandas as pd

file_path = "/Users/junior/Documents/github/ecommerce/transactions.json"

df = pd.read_json(file_path)
print(df.columns)
print("################")