# 50_export.py
# ------------------------------------------------------------
# Step 50 (Final): Export final csv_plotdata.csv into SQLite DB
#
# Table name: z4_plot_data
# Behavior:
#   • Replaces existing table if it exists.
#   • Cleans column names for SQLite compatibility.
#
# Input  : 100_Data/csv_plotdata.csv
# Output : Table "z4_plot_data" in 100_Data/pdf_data.sqlite
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

input_path = data_dir / "csv_plotdata.csv"
db_path = data_dir / "pdf_data.sqlite"

print("📤 Step 50: Exporting final csv_plotdata.csv to SQLite database...")

# ------------------------------------------------------------
# Validate input
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun Step 32 first.")

# Create database if missing
if not db_path.exists():
    print(f"⚠️ Database not found: creating new file → {db_path}")
    db_path.touch()

# ------------------------------------------------------------
# Load CSV
# ------------------------------------------------------------
print("🔍 Reading final plot data CSV...")
df = pd.read_csv(input_path)

# Clean column names (SQLite safe)
df.columns = [c.strip().replace(" ", "_").replace("-", "_") for c in df.columns]

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns from {input_path.name}")

# ------------------------------------------------------------
# Write to SQLite
# ------------------------------------------------------------
table_name = "z4_plot_data"
print(f"💾 Writing data to SQLite → {db_path.name} (table: {table_name})...")

conn = sqlite3.connect(db_path)
df.to_sql(table_name, conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Export complete → {db_path.name}")
print(f"📊 Table '{table_name}' now contains {len(df):,} rows and {len(df.columns)} columns.")
print("🎯 You can inspect it in VS Code or DB Browser for SQLite.")