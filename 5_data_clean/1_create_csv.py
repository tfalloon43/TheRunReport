# 1_create_csv.py
# ------------------------------------------------------------
# Step 1 of Data Clean Pipeline
# Extracts the full 'z2_create_datatable' table from the database
# and saves it as a working CSV called csv_reduce.csv.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 1: Extracting z2_create_datatable → csv_reduce.csv")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

db_path = data_dir / "pdf_data.sqlite"
output_path = data_dir / "csv_reduce.csv"

# ------------------------------------------------------------
# Validate
# ------------------------------------------------------------
if not db_path.exists():
    raise FileNotFoundError(f"❌ Database not found: {db_path}")

# ------------------------------------------------------------
# Extract data
# ------------------------------------------------------------
print(f"🔍 Connecting to {db_path.name} ...")
conn = sqlite3.connect(db_path)

# Load the full 2_create_datatable table
df = pd.read_sql("SELECT * FROM z2_create_datatable", conn)
conn.close()

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns from 'z2_create_datatable'.")

# ------------------------------------------------------------
# Save to CSV
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
print(f"💾 Exported full dataset → {output_path}")
print(f"🎯 You can now use this file for data reduction and cleaning.")
