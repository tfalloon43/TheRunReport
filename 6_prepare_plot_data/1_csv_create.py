# 1_csv_create.py
# ------------------------------------------------------------
# Step 1: Export the "3_data_clean" table from pdf_data.sqlite
# into a CSV file called csv_plotdata.csv (saved in 100_Data).
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 1: Creating csv_plotdata.csv from z3_data_clean...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

db_path = data_dir / "pdf_data.sqlite"
output_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Validate paths
# ------------------------------------------------------------
if not db_path.exists():
    raise FileNotFoundError(f"❌ Database not found at: {db_path}")

print(f"📂 Database found at: {db_path}")

# ------------------------------------------------------------
# Read data from SQLite
# ------------------------------------------------------------
try:
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM 'z3_data_clean';", conn)
except Exception as e:
    raise RuntimeError(f"❌ Error reading table 'z3_data_clean' from database: {e}")

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns from z3_data_clean")

# ------------------------------------------------------------
# Save as CSV in 100_Data
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
print(f"💾 Exported → {output_path}")

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
print("✅ Step 1 complete — csv_plotdata.csv created successfully in 100_Data.")
