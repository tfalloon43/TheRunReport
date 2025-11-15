# 51_export.py
# ------------------------------------------------------------
# Export unified weekly 10-year + current-year CSV into SQLite
#
#   CSV Source : 100_Data/weekly_unified_long.csv
#   SQLite DB  : 100_Data/pdf_data.sqlite
#   Table Name : z5_10y_averages_currentyear
#
# Behavior:
#   • Creates DB if missing
#   • Replaces existing table if already present
#   • Cleans column names for SQLite compatibility
# ------------------------------------------------------------

import pandas as pd
import sqlite3
from pathlib import Path

print("📤 Step 50: Exporting unified weekly dataset → SQLite (z5_10y_averages_currentyear)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir      = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

input_path = data_dir / "weekly_unified_long.csv"
db_path    = data_dir / "pdf_data.sqlite"
table_name = "z5_10y_averages_currentyear"

# ------------------------------------------------------------
# Validate CSV exists
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing unified CSV: {input_path}\nRun 9_unify_weekly_data.py first.")

# Create DB if missing
if not db_path.exists():
    print(f"⚠️ SQLite database not found — creating new file: {db_path}")
    db_path.touch()

# ------------------------------------------------------------
# Load CSV
# ------------------------------------------------------------
print("🔍 Reading unified weekly CSV...")
df = pd.read_csv(input_path)

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns from {input_path.name}")

# ------------------------------------------------------------
# Clean column names for SQLite
# ------------------------------------------------------------
df.columns = [
    c.strip()
     .replace(" ", "_")
     .replace("-", "_")
     .replace("/", "_")
     .replace("(", "")
     .replace(")", "")
    for c in df.columns
]

# ------------------------------------------------------------
# Write to SQLite
# ------------------------------------------------------------
print(f"💾 Writing to SQLite table → {table_name} ...")

conn = sqlite3.connect(db_path)
df.to_sql(table_name, conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print("✅ Export complete!")
print(f"📚 SQLite DB: {db_path.name}")
print(f"📝 Table '{table_name}' now contains {len(df):,} rows.")
print("🎯 You can inspect it in VS Code or DB Browser for SQLite.")