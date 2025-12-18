# step34_cleanup1.py
# ------------------------------------------------------------
# Step 34: Cleanup Pass 1
#
# Remove rows where x_count == 1 (singletons inside a biological
# identity). All other rows are kept unchanged.
# Output: Updates Escapement_PlotPipeline directly.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 34: Cleanup Pass 1 — Removing rows where x_count == 1...")

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
print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# Normalize column names to underscore schema if needed
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
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "date_iso", "x_count", "Adult_Total"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["x_count"] = pd.to_numeric(df["x_count"], errors="coerce").fillna(0).astype(int)
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

before = len(df)

cleaned = df[df["x_count"] != 1].reset_index(drop=True)

after = len(cleaned)
removed = before - after

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
cleaned = reorder_for_output(cleaned)
cleaned.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup Pass 1 Complete!")
print(f"🧹 Removed {removed:,} rows from short-run clusters x_count ==3.")
print(f"📊 Final dataset size: {after:,} rows.")
print("🏁 Escapement_PlotPipeline table updated successfully.")
