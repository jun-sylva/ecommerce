import sqlite3

# Connexion à la base de données (ou création si elle n'existe pas)
conn = sqlite3.connect("ecommerce.db")
cursor = conn.cursor()

# Récupérer la liste des tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# Afficher les tables
print("Tables dans la base de données :")
for table in tables:
    print(table[0])

# Fermer la connexion
conn.close()