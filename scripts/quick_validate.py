#!/usr/bin/env python3
"""Quick validation of corrected dataset."""

import json
import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_PATH = os.path.join(BASE_DIR, "data", "database", "chichewa_text2sql.db")
DATASET_PATH = os.path.join(BASE_DIR, "data", "train", "train_corrected.json")

# Load data
data = json.load(open(DATASET_PATH, 'r', encoding='utf-8'))
db = sqlite3.connect(DATABASE_PATH)
cursor = db.cursor()

valid = 0
syntax_errors = 0
exec_errors = []

for i, d in enumerate(data):
    try:
        cursor.execute(d['sql_statement'])
        result = cursor.fetchall()
        valid += 1
    except sqlite3.Error as e:
        syntax_errors += 1
        exec_errors.append({
            'index': i,
            'table': d.get('table'),
            'error': str(e),
            'sql': d['sql_statement'][:100]
        })

db.close()

print("=" * 60)
print("CORRECTED DATASET VALIDATION")
print("=" * 60)
print(f"Total instances: {len(data)}")
print(f"Valid (executable): {valid}")
print(f"Syntax/execution errors: {syntax_errors}")
print(f"Success rate: {valid/len(data)*100:.2f}%")

if exec_errors:
    print("\nErrors found:")
    for err in exec_errors[:10]:
        print(f"  [{err['index']}] {err['table']}: {err['error']}")

# Check distribution
tables = {}
for d in data:
    t = d['table']
    tables[t] = tables.get(t, 0) + 1

print("\nDistribution by table:")
for t, c in sorted(tables.items()):
    print(f"  {t}: {c}")

print("=" * 60)
