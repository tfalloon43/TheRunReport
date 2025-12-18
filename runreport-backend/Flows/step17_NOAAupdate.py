# 17_NOAAupdate.py
# ------------------------------------------------------------
# Step 17: Add id column + remove timestamp_dt from NOAA_flows
#
# • Reads NOAA_flows table (output of Step 16)
# • Adds "id" column (1, 2, 3, ..., N) as FIRST column
# • Deletes "timestamp_dt"
# • Ensures numeric types for stage_ft and flow_cfs
#
# Input/Output : local.db table NOAA_flows
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
import sqlite3

print("🌊 Step 17: Adding ID + removing timestamp_dt from NOAA flows…")

# ------------------------------------------------------------
# DB PATH / TABLE
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
TABLE_NOAA_FLOWS = "NOAA_flows"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
print(f"🗄️ Using DB → {db_path}")
with sqlite3.connect(db_path) as conn:
    try:
        df = pd.read_sql_query(f"SELECT * FROM [{TABLE_NOAA_FLOWS}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing table [{TABLE_NOAA_FLOWS}] in local.db: {e}")

print(f"📂 Loaded {len(df):,} rows from [{TABLE_NOAA_FLOWS}]")

# ------------------------------------------------------------
# Remove timestamp_dt
# ------------------------------------------------------------
if "timestamp_dt" in df.columns:
    df = df.drop(columns=["timestamp_dt"])
    print("🗑️ Removed 'timestamp_dt' column.")
else:
    print("⚠️ Column 'timestamp_dt' not found — skipping removal.")

# ------------------------------------------------------------
# Convert numeric columns to numeric
# ------------------------------------------------------------
numeric_cols = ["stage_ft", "flow_cfs"]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"🔢 Converted '{col}' to numeric.")
    else:
        print(f"⚠️ Numeric column '{col}' not found — skipping.")

# ------------------------------------------------------------
# Add ID column
# ------------------------------------------------------------
df.insert(0, "id", range(1, len(df) + 1))
print("🆔 Added 'id' column as first column.")

print(f"📊 Final columns: {list(df.columns)}")

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
with sqlite3.connect(db_path) as conn:
    df.to_sql(TABLE_NOAA_FLOWS, conn, if_exists="replace", index=False)

print(f"🔄 Updated [{TABLE_NOAA_FLOWS}] with id + no timestamp_dt")
print("✅ Step 17 complete.")
