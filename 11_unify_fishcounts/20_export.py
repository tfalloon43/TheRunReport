# 20_export.py
# ------------------------------------------------------------
# Step 20: Export fishcount datasets into SQLite database
#
# Exports:
#   • csv_unify_fishcounts.csv  →  z6_plotable_hatcherycounts
#   • columbiadaily_raw.csv     →  z7_plotable_columbiacounts
#
# Behavior:
#   • Creates DB if missing
#   • Replaces tables if they already exist
#   • Cleans column names for SQLite compatibility
#
# Input  :
#     100_Data/csv_unify_fishcounts.csv
#     100_Data/columbiadaily_raw.csv
#
# Output :
#     Tables in 100_Data/pdf_data.sqlite:
#         - z6_plotable_hatcherycounts
#         - z7_plotable_columbiacounts
# ------------------------------------------------------------

import pandas as pd
import sqlite3
from pathlib import Path

print("📤 Step 20: Exporting hatchery + Columbia datasets to SQLite database...\n")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

db_path = data_dir / "pdf_data.sqlite"

csv_hatch  = data_dir / "csv_unify_fishcounts.csv"
csv_col    = data_dir / "columbiadaily_raw.csv"

# ------------------------------------------------------------
# Validate files
# ------------------------------------------------------------
missing = []
if not csv_hatch.exists():
    missing.append(str(csv_hatch))
if not csv_col.exists():
    missing.append(str(csv_col))

if missing:
    raise FileNotFoundError(
        "❌ Missing required input CSV(s):\n" + "\n".join(f"   • {m}" for m in missing)
    )

# Ensure DB exists
if not db_path.exists():
    print(f"⚠️ Database not found — creating new file → {db_path}")
    db_path.touch()

# ------------------------------------------------------------
# Helper to clean column names
# ------------------------------------------------------------
def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        c.strip()
         .replace(" ", "_")
         .replace("-", "_")
         .replace("/", "_")
         .replace("(", "")
         .replace(")", "")
        for c in df.columns
    ]
    return df

# ------------------------------------------------------------
# Load CSVs
# ------------------------------------------------------------
print("🔍 Reading hatchery + Columbia_source CSVs...")

df_hatch = pd.read_csv(csv_hatch)
df_hatch = clean_cols(df_hatch)
print(f"   🐟 Loaded hatchery unified dataset → {len(df_hatch):,} rows")

df_col = pd.read_csv(csv_col)
df_col = clean_cols(df_col)
print(f"   🌊 Loaded Columbia daily dataset → {len(df_col):,} rows\n")

# ------------------------------------------------------------
# Write to SQLite
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)

table_hatch = "z6_plotable_hatcherycounts"
table_col   = "z7_plotable_columbiacounts"

print(f"💾 Writing → {db_path.name}")

df_hatch.to_sql(table_hatch, conn, if_exists="replace", index=False)
print(f"   ✔ Saved hatchery data as table '{table_hatch}'")

df_col.to_sql(table_col, conn, if_exists="replace", index=False)
print(f"   ✔ Saved Columbia data as table '{table_col}'")

conn.close()

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print("\n✅ Export complete!")
print(f"📊 {table_hatch}: {len(df_hatch):,} rows")
print(f"📊 {table_col}:   {len(df_col):,} rows")
print("🎯 Data now available in pdf_data.sqlite for plotting + app use.")