# step40_cleanup4.py
# ------------------------------------------------------------
# Step 40: Cleanup Pass 4
#
# Remove rows flagged as short runs (by_short4 == "X"), but
# always keep rows from the current year. Updates the table
# Escapement_PlotPipeline in place.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

# ------------------------------------------------------------
# Reorder helper
# ------------------------------------------------------------
def reorder_for_output(df):
    sort_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total"]
    missing = [c for c in sort_cols if c not in df.columns]
    if missing:
        return df
    df = df.copy()
    df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
    df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
    return df.sort_values(
        by=sort_cols,
        ascending=[True, True, True, True, True, False],
        na_position="last",
        kind="mergesort",
    )


print("🏗️ Step 40: Removing by_short4 == 'X' rows (keep current year)...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD TABLE
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)
print(f"✅ Loaded {len(df):,} rows")

# Normalize column names
rename_map = {
    "Adult Total": "Adult_Total",
    "Jack Total": "Jack_Total",
    "Total Eggtake": "Total_Eggtake",
    "On Hand Adults": "On_Hand_Adults",
    "On Hand Jacks": "On_Hand_Jacks",
    "Lethal Spawned": "Lethal_Spawned",
    "Live Spawned": "Live_Spawned",
    "Live Shipped": "Live_Shipped",
}
df = df.rename(columns=rename_map)

# ------------------------------------------------------------
# VALIDATE REQUIRED COLUMNS
# ------------------------------------------------------------
required = ["date_iso", "by_short4", "Adult_Total"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

current_year = datetime.now().year
is_current_year = df["date_iso"].dt.year == current_year

before = len(df)

# Keep current-year rows, and drop prior-year rows with by_short4 == "X"
df_clean = df[(is_current_year) | (df["by_short4"] != "X")].reset_index(drop=True)

after = len(df_clean)
removed = before - after

# Reorder for consistent output
df_clean = reorder_for_output(df_clean)

# ------------------------------------------------------------
# WRITE BACK TO DB
# ------------------------------------------------------------
print("💾 Writing cleaned results back to Escapement_PlotPipeline...")
df_clean.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup 4 Complete!")
print(f"🧹 Removed {removed:,} rows where by_short4 == 'X' (prior years only).")
print(f"📊 Final rows: {after:,}")
print("🏁 by_short4 cleanup applied.")
