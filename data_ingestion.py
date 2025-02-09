import pandas as pd
from sqlalchemy import create_engine

# Connexion à SQLite
DB_NAME = 'ecommerce.db'
db_url = f'sqlite:///{DB_NAME}'
engine = create_engine(db_url)

# Chargement des fichiers JSON
def load_json(file_path):
    return pd.read_json(file_path, lines=True)

customers = load_json("./data/customers.json")
orders = load_json("./data/orders.json")
sales = load_json("./data/sales.json")
products = load_json("./data/products.json")

# Nettoyage des données (exemple : suppression des doublons et valeurs nulles)
customers.drop_duplicates(inplace=True)
orders.drop_duplicates(inplace=True)
sales.drop_duplicates(inplace=True)
products.drop_duplicates(inplace=True)

customers.dropna(inplace=True)
orders.dropna(inplace=True)
sales.dropna(inplace=True)
products.dropna(inplace=True)

# Normalisation des noms de colonnes
customers.columns = customers.columns.str.lower()
orders.columns = orders.columns.str.lower()
sales.columns = sales.columns.str.lower()
products.columns = products.columns.str.lower()

# Sauvegarde dans SQLite

# customers.to_sql("customers", engine, if_exists="replace", index=False)
#orders.to_sql("orders", engine, if_exists="replace", index=False)
#sales.to_sql("sales", engine, if_exists="replace", index=False)
#products.to_sql("products", engine, if_exists="replace", index=False)

print("Données chargées avec succès dans SQLite !")
