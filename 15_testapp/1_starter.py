import sqlite3
from pathlib import Path

db_path = Path("/Users/thomasfalloon/Desktop/TheRunReport/100_Data/pdf_data.sqlite")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("Tables:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(tables)

print("\nSchema for each table:")
for (table_name,) in tables:
    print(f"\n--- {table_name} ---")
    cursor.execute(f"PRAGMA table_info('{table_name}');")
    for col in cursor.fetchall():
        print(col)

conn.close()