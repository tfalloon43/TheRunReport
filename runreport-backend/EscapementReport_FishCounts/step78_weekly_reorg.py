# step78_weekly_reorg.py
# ------------------------------------------------------------
# Step 78: Reorganize weekly counts into long format for plotting
#
# Reads EscapementReports_weeklycounts and melts basinfamily
# columns into long format:
#   MM-DD | metric_type | identifier (basinfamily) | value
# Adds a sortable date_obj for convenience, writes to
# EscapementReport_PlotData in local.db.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🔗 Step 78: Reorganizing weekly counts into long-format plot data...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD WEEKLY TABLE
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
weekly_df = pd.read_sql_query("SELECT * FROM EscapementReports_weeklycounts;", conn)
print(f"✅ Loaded {len(weekly_df):,} rows and {len(weekly_df.columns)} columns from EscapementReports_weeklycounts")

# ------------------------------------------------------------
# VALIDATE
# ------------------------------------------------------------
required = ["metric_type", "MM-DD"]
missing = [c for c in required if c not in weekly_df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in EscapementReports_weeklycounts: {missing}")

value_cols = [c for c in weekly_df.columns if c not in ("metric_type", "MM-DD")]
if not value_cols:
    raise ValueError("❌ No basinfamily value columns found to melt.")

# Normalize
weekly_df["metric_type"] = weekly_df["metric_type"].astype(str).str.strip()
weekly_df["MM-DD"] = weekly_df["MM-DD"].astype(str).str.strip()

# ------------------------------------------------------------
# MELT TO LONG FORMAT
# ------------------------------------------------------------
long_df = weekly_df.melt(
    id_vars=["metric_type", "MM-DD"],
    value_vars=value_cols,
    var_name="identifier",
    value_name="value",
)

long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")

# Build sortable date for plotting/order (fixed year)
long_df["date_obj"] = pd.to_datetime("2024-" + long_df["MM-DD"], errors="coerce")

# ------------------------------------------------------------
# WRITE OUTPUT TABLE
# ------------------------------------------------------------
# NOTE: Downstream steps (79+) expect EscapementReport_PlotData (singular).
long_df.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Step 78 complete — EscapementReport_PlotData created.")
print(f"📊 Rows: {len(long_df):,}")
print(f"🔢 Identifiers: {len(value_cols)}")
