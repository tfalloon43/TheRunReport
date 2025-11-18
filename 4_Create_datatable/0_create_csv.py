# 0_create_csv.py
# ------------------------------------------------------------
# Step 0: Export the "z1_pdf_lines" table from pdf_data.sqlite
# into a CSV file called csv_recent.csv (saved in 100_Data).
#
# This represents the *latest parsed PDF lines* from WDFW
# before any cleaning, filtering, or transformations.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 0: Creating csv_recent.csv from z1_pdf_lines...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

db_path = data_dir / "pdf_data.sqlite"
output_path = data_dir / "csv_recent.csv"
compare_path = data_dir / "0_create_csv.csv"

# ------------------------------------------------------------
# Validate database path
# ------------------------------------------------------------
if not db_path.exists():
    raise FileNotFoundError(f"❌ Database not found at: {db_path}")

print(f"📂 Database found at: {db_path}")

# ------------------------------------------------------------
# Read data from SQLite
# ------------------------------------------------------------
try:
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM 'z1_pdf_lines';", conn)
except Exception as e:
    raise RuntimeError(f"❌ Error reading table 'z1_pdf_lines' from database: {e}")

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns from z1_pdf_lines")

# ------------------------------------------------------------
# Save CSVs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
print(f"💾 Exported → {output_path}")

df.to_csv(compare_path, index=False)
print(f"💾 Exported comparison snapshot → {compare_path}")

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
print("✅ Step 0 complete — csv_recent.csv and 0_create_csv.csv created successfully in 100_Data.")
