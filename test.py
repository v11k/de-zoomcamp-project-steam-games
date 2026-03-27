import json

path = "data/raw/steam_catalog/steam_catalog_20260325T201556Z.jsonl"

count = 0
bad = 0

with open(path, "r", encoding="utf-8") as f:
    for line in f:
        count += 1
        try:
            json.loads(line)
        except json.JSONDecodeError:
            bad += 1

print("rows:", count)
print("bad rows:", bad)