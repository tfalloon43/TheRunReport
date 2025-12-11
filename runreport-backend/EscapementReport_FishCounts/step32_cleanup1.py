# step32_cleanup1.py
# ------------------------------------------------------------
# Step 32: Cleanup Pass 1
#
# Removes duplicate Adult Total values inside spillover
# short-run clusters (x_count ≥ 3).
#
# Rules:
#   • Work inside biological identity:
#         facility, species, Stock, Stock_BO
#   • If x_count < 3 → untouched
#   • If x_count ≥ 3:
#         - Identify contiguous block with same x_count
#         - For duplicate Adult Total values:
#               keep the earliest (oldest) date_iso
#               remove all others
#
# Output: Updates Escapement_PlotPipeline directly.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 32: Cleanup Pass 1 — Removing duplicate Adult Total within x_count ≥ 3 clusters...")

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

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# ------------------------------------------------------------
# CORE LOGIC
# ------------------------------------------------------------
def process_group(g):
    g = g.sort_values("date_iso").reset_index(drop=True)
    n = len(g)

    keep_mask = [True] * n
    i = 0

    while i < n:
        xval = g.loc[i, "x_count"]

        if xval < 3:
            i += 1
            continue

        # Find contiguous x_count cluster
        j = i
        while j < n and g.loc[j, "x_count"] == xval:
            j += 1

        cluster = g.loc[i:j]

        # Mark duplicates (except earliest)
        dupes = cluster.duplicated(subset=["Adult_Total"], keep="first")

        for idx in cluster.index[dupes]:
            keep_mask[idx] = False

        i = j

    return g.loc[keep_mask]


# ------------------------------------------------------------
# APPLY CLEANUP
# ------------------------------------------------------------
before = len(df)

cleaned = (
    df.groupby(group_cols, group_keys=False)
      .apply(process_group)
      .reset_index(drop=True)
)

after = len(cleaned)
removed = before - after

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
cleaned.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup Pass 1 Complete!")
print(f"🧹 Removed {removed:,} rows from short-run clusters (x_count ≥ 3).")
print(f"📊 Final dataset size: {after:,} rows.")
print("🏁 Escapement_PlotPipeline table updated successfully.")
