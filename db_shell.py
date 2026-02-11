import sqlite3
conn = sqlite3.connect('data/database/chichewa_text2sql.db')
c = conn.cursor()

def q(sql):
    """Run a SQL query and return results."""
    return c.execute(sql).fetchall()

print("=" * 50)
print("DATABASE CONNECTED: chichewa_text2sql.db")
print("=" * 50)
print("\nTables: production, population, food_insecurity,")
print("        commodity_prices, mse_daily")
print("\nRun queries with: q(\"YOUR SQL HERE\")")
print("Example: q(\"SELECT * FROM production LIMIT 5\")")
print("=" * 50)
