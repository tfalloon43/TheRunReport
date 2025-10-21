# 10_export.py
# ------------------------------------------------------------
# Exports the reduced CSV (csv_reduce.csv) into the SQLite database.
# Table name: 3_data_clean
# Behavior:
#   • Replaces the existing table if it already exists.
#   • Ensures column names and types are preserved.
# Input  : 100_Data/csv_reduce.csv
# Output : Table "3_data_clean" in pdf_data.sqlite
# ------------------------------------------------------------

import os
import sqlite3
import pandas as pd
from pathlib import Path

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_reduce.csv"
db_path = data_dir / "pdf_data.sqlite"

print("📤 Step 10: Exporting csv_reduce.csv to SQLite (table: z3_data_clean)...")

# ------------------------------------------------------------
# Validate inputs
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input CSV file: {input_path}")

if not db_path.exists():
    raise FileNotFoundError(f"❌ Missing database: {db_path}\nMake sure pdf_data.sqlite exists in 100_Data/.")

# ------------------------------------------------------------
# Load CSV
# ------------------------------------------------------------
print("🔍 Reading CSV file...")
df = pd.read_csv(input_path)

# Clean up column names (SQLite-safe)
df.columns = [c.strip().replace(" ", "_").replace("-", "_") for c in df.columns]

print(f"✅ Loaded {len(df):,} rows × {len(df.columns)} columns from {input_path.name}")

# ------------------------------------------------------------
# Write to SQLite
# ------------------------------------------------------------
print("💾 Writing to SQLite database...")

with sqlite3.connect(db_path) as conn:
    df.to_sql("z3_data_clean", conn, if_exists="replace", index=False)

print("✅ Data successfully exported.")
print(f"📊 Table name: z3_data_clean")
print(f"📁 Database: {db_path}")
print(f"🔢 Total rows written: {len(df):,}")
print("🎯 You can inspect this table in VS Code or DB Browser for SQLite.")