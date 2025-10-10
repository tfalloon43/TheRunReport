# export_pdf_lines.py
import sqlite3
import pandas as pd

# Path to your SQLite database (same directory as your other scripts)
db_path = "pdf_data.sqlite"

# Connect to the database
conn = sqlite3.connect(db_path)

# Read the pdf_lines table into a DataFrame
df = pd.read_sql_query("SELECT * FROM pdf_lines ORDER BY id", conn)

# Save as CSV
output_csv = "pdf_lines.csv"
df.to_csv(output_csv, index=False)

conn.close()

print(f"✅ Exported {len(df)} rows from pdf_lines → {output_csv}")