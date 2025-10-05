import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect("pdf_data.sqlite")

# Corrected query
query = """
SELECT pdf_name, hatchery, stock, adult_total, species, date
FROM escapement_data_cleaned
WHERE stock = 'IDK'
"""
# WHERE stock = 'IDK'

df = pd.read_sql_query(query, conn)

# Display the filtered rows
print(df)
df.to_csv("view_data_output_IDK.csv", index=False)

conn.close()