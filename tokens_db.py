import sqlite3

conn = sqlite3.connect("tokens.db")
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables in the database:", tables)

# Check what data is inside each table
for table in tables:
    cursor.execute(f"SELECT * FROM {table[0]} LIMIT 5;")
    rows = cursor.fetchall()
    print(f"Sample data from {table[0]}:", rows)

conn.close()
