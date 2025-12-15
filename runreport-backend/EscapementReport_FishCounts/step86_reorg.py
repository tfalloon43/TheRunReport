# step86_reorg.py
# ------------------------------------------------------------
# Step 86: Final reorg for EscapementReport_PlotData
#
# - Drop identifier
# - Sort by river → Species_Plot → MM-DD
# - Add sequential id column
# - Convert current_year, previous_year, 10_year to numeric
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 86: Reorganizing EscapementReport_PlotData (drop identifier, sort, add id, numeric conversions)...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM EscapementReport_PlotData;", conn)
print(f"📂 Loaded {len(df):,} rows from EscapementReport_PlotData")

# ------------------------------------------------------------
# DROP IDENTIFIER IF PRESENT
# ------------------------------------------------------------
if "identifier" in df.columns:
    df = df.drop(columns=["identifier"])
    print("🗑️ Dropped 'identifier' column.")

# ------------------------------------------------------------
# NORMALIZE AND SORT
# ------------------------------------------------------------
for col in ["river", "Species_Plot", "MM-DD"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# Sort with date ordering using fixed year
if "MM-DD" in df.columns:
    df["date_obj"] = pd.to_datetime("2024-" + df["MM-DD"], errors="coerce")
    sort_cols = [c for c in ["river", "Species_Plot", "date_obj"] if c in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)
    df = df.drop(columns=["date_obj"])

# ------------------------------------------------------------
# ADD ID COLUMN
# ------------------------------------------------------------
df.insert(0, "id", range(1, len(df) + 1))
print("🆔 Added 'id' column as first column.")

# ------------------------------------------------------------
# CONVERT METRIC COLUMNS TO NUMERIC
# ------------------------------------------------------------
metric_cols = [c for c in ["current_year", "previous_year", "10_year"] if c in df.columns]
if metric_cols:
    df[metric_cols] = df[metric_cols].apply(pd.to_numeric, errors="coerce")
    print(f"🔢 Converted metric columns to numeric: {metric_cols}")
else:
    print("⚠️ No metric columns found to convert.")

# ------------------------------------------------------------
# WRITE BACK
# ------------------------------------------------------------
df.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Step 86 complete — EscapementReport_PlotData reorganized.")
print(f"📊 Rows: {len(df):,}")
print(f"🔢 Columns: {list(df.columns)}")
