#!/usr/bin/env python3
"""Inspect database schema in detail."""

import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_PATH = os.path.join(BASE_DIR, "data", "database", "chichewa_text2sql.db")

conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

tables = ['production', 'population', 'mse_daily', 'commodity_prices', 'food_insecurity']

for table in tables:
    print(f"\n=== {table.upper()} ===")
    cursor.execute(f"PRAGMA table_info({table})")
    columns = cursor.fetchall()
    for col in columns:
        print(f"  {col[1]} ({col[2]})")
    
    # Sample data
    cursor.execute(f"SELECT * FROM {table} LIMIT 2")
    rows = cursor.fetchall()
    if rows:
        print(f"  Sample: {rows[0][:5]}...")

conn.close()
