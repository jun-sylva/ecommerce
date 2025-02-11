import json

file_path = "/Users/junior/Documents/github/ecommerce/transactions.json"
new_path = "/Users/junior/Documents/github/ecommerce/transactions_fixed.json"

with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

# Fix incorrect keys inside the shopping_cart list
for transaction in data:
    for item in transaction["shopping_cart"]:
        if "vendor: " in item:
            item["vendor"] = item.pop("vendor: ")

# Save the corrected JSON
with open(new_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)

print("Fixed JSON saved as transactions_fixed.json")
