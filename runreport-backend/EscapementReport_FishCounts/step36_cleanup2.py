# step36_cleanup2.py
# ------------------------------------------------------------
# Step 36: Cleanup Pass 2
#
# Logic:
#   • Work inside DB table Escapement_PlotPipeline
#   • Group by biological identity + by_adult2:
#         facility, species, Stock, Stock_BO, by_adult2
#   • Within each group, if rows share the same Adult_Total,
#     keep only the row with the earliest date_iso.
#   • Afterward, drop intermediate columns:
#       day_diff, adult_diff, by_adult, by_adult_length,
#       by_short, x_count
#
# Output: Escapement_PlotPipeline rewritten in-place
# ------------------------------------------------------------

import sqlite3
import pandas as pd
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


print("🏗️ Step 36: Removing duplicate Adult_Total within by_adult2 groups (earliest date_iso wins)...")

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
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# Normalize column names to underscore schema if needed
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
# CHECK REQUIRED COLUMNS
# ------------------------------------------------------------
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "date_iso", "Adult_Total", "by_adult2"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# TYPE NORMALIZATION
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

group_cols = ["facility", "species", "Stock", "Stock_BO", "by_adult2"]

# ------------------------------------------------------------
# CORE CLEANUP LOGIC
# ------------------------------------------------------------
def dedupe_adult_total(g):
    """Within a group, keep earliest date_iso for duplicate Adult_Total."""
    g = g.sort_values(["Adult_Total", "date_iso", "pdf_date"], na_position="last").reset_index(drop=True)
    keep_mask = ~g.duplicated(subset=["Adult_Total"], keep="first")
    return g.loc[keep_mask]

# ------------------------------------------------------------
# APPLY PER GROUP
# ------------------------------------------------------------
before = len(df)

df_clean = (
    df.groupby(group_cols, group_keys=False)
      .apply(dedupe_adult_total)
      .reset_index(drop=True)
)

after = len(df_clean)
removed = before - after

# Drop intermediate columns
drop_cols = ["day_diff", "adult_diff", "by_adult", "by_adult_length", "by_short", "x_count"]
drop_cols = [c for c in drop_cols if c in df_clean.columns]
if drop_cols:
    df_clean = df_clean.drop(columns=drop_cols)

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df_clean = reorder_for_output(df_clean)

df_clean.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup2 complete!")
print(f"🧹 Removed {removed:,} rows with duplicate Adult_Total within by_adult2 groups.")
print(f"📊 Final row count: {after:,}")
print(f"📁 Escapement_PlotPipeline updated in database.")
