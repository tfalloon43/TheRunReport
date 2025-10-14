# 1_date_extract.py
# ------------------------------------------------------------
# Step 1: Extracts dates from text_line and saves both
# - 1_date_extract_output.csv
# - csv_recent.csv (latest snapshot)
# ------------------------------------------------------------

import sqlite3
import pandas as pd
import re
import os

# --- Load data from SQLite ---
db_path = os.path.join(os.path.dirname(__file__), "..", "pdf_data.sqlite")
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM pdf_lines ORDER BY id", conn)
conn.close()

# --- Extract date ---
def extract_date(text):
    if not isinstance(text, str):
        return None
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", text)
    return m.group(1) if m else None

df["date"] = df["text_line"].apply(extract_date)

# --- Save outputs ---
step_output = os.path.join(os.path.dirname(__file__), "1_date_extract_output.csv")
recent_output = os.path.join(os.path.dirname(__file__), "csv_recent.csv")

df.to_csv(step_output, index=False)
df.to_csv(recent_output, index=False)

print("✅ Step 1 complete → Saved:")
print(f"   {step_output}")
print(f"   {recent_output} (latest snapshot)")