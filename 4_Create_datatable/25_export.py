# 25_export.py
# ------------------------------------------------------------
# Exports the final csv_recent.csv into the SQLite database.
# Table name: create_datatable
# Behavior:
#   • Replaces the existing table if it already exists.
#   • Ensures column names and types are preserved.
# Input  : 100_Data/csv_recent.csv (latest output from create_datatable pipeline)
# Output : Table "create_datatable" in 100_Data/pdf_data.sqlite
# ------------------------------------------------------------

import pandas as pd
import sqlite3
from pathlib import Path

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

input_path = data_dir / "csv_recent.csv"
db_path = data_dir / "pdf_data.sqlite"

print("📤 Step 20: Exporting csv_recent.csv to SQLite database...")

# ------------------------------------------------------------
# Validate inputs
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun previous step first.")

if not db_path.exists():
    raise FileNotFoundError(f"❌ Database not found: {db_path}\nMake sure pdf_data.sqlite exists in 100_Data.")

# ------------------------------------------------------------
# Load CSV
# ------------------------------------------------------------
print("🔍 Reading CSV data...")
df = pd.read_csv(input_path)

# Clean up column names for SQLite (no spaces or hyphens)
df.columns = [c.strip().replace(" ", "_").replace("-", "_") for c in df.columns]

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns from {input_path.name}")

# ------------------------------------------------------------
# Write to SQLite
# ------------------------------------------------------------
print(f"💾 Writing data to SQLite → {db_path.name} (table: z2_create_datatable) ...")

conn = sqlite3.connect(db_path)
df.to_sql("z2_create_datatable", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
print(f"✅ Data successfully exported to '{db_path.name}'")
print(f"📊 Table 'z2_create_datatable' now contains {len(df):,} rows and {len(df.columns)} columns.")
print("🎯 You can inspect it in VS Code or DB Browser for SQLite.")