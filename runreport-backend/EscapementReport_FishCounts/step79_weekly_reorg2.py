# step79_weekly_reorg2.py
# ------------------------------------------------------------
# Step 79: Pivot weekly plot data into wide format
# 
# Input : EscapementReport_PlotData (long, metric_type column)
# Output: EscapementReport_PlotData (wide) with columns:
#         MM-DD | identifier | current_year | previous_year | 10_year
# Drops date_obj and pivots metric_type values into dedicated columns.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🔄 Step 79: Pivoting weekly plot data to wide format...")

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
print(f"✅ Loaded {len(df):,} rows from EscapementReport_PlotData")

# ------------------------------------------------------------
# CLEANUP AND VALIDATE
# ------------------------------------------------------------
if "date_obj" in df.columns:
    df = df.drop(columns=["date_obj"])

required = ["metric_type", "MM-DD", "identifier", "value"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# Normalize types
df["metric_type"] = df["metric_type"].astype(str).str.strip()
df["MM-DD"] = df["MM-DD"].astype(str).str.strip()
df["identifier"] = df["identifier"].astype(str).str.strip()
df["value"] = pd.to_numeric(df["value"], errors="coerce")

# ------------------------------------------------------------
# PIVOT TO WIDE FORMAT
# ------------------------------------------------------------
pivot = (
    df.pivot_table(
        index=["MM-DD", "identifier"],
        columns="metric_type",
        values="value",
        aggfunc="sum",
    )
    .reset_index()
)

# Ensure expected metric columns exist
for col in ["current_year", "previous_year", "10_year"]:
    if col not in pivot.columns:
        pivot[col] = pd.NA

# Reorder columns
pivot = pivot[["MM-DD", "identifier", "current_year", "previous_year", "10_year"]]

# Sort by date then identifier (using fixed year for ordering)
pivot["date_obj"] = pd.to_datetime("2024-" + pivot["MM-DD"], errors="coerce")
pivot = pivot.sort_values(["date_obj", "identifier"]).drop(columns=["date_obj"]).reset_index(drop=True)

# ------------------------------------------------------------
# WRITE BACK
# ------------------------------------------------------------
pivot.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Step 79 complete — EscapementReport_PlotData pivoted to wide format.")
print(f"📊 Rows: {len(pivot):,}")
print(f"🔢 Columns: {list(pivot.columns)}")
