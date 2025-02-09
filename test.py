import pandas as pd

file_path = "./data/transactions.json"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()
    print(f"Contenu de {file_path}:\n", content[:500])  # Affiche les 500 premiers caractères

df = pd.read_json(file_path, lines=True)
print(df.head())