import json

file_path = "/transactions.json"

try:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print("JSON is valid!")
except json.JSONDecodeError as e:
    print(f"JSON Error: {e}")