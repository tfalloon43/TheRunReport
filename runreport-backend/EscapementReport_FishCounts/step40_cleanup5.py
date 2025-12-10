# step40_cleanup5.py
# ------------------------------------------------------------
# Step 40 (Cleanup v5): Reorder dataset by biological identity
# and biological year (by_adult5) directly in the database.
#
# Rules:
#   - Work within facility/species/Stock/Stock_BO groups
#   - Sort rows by:
#         1. by_adult5 (biological year)
#         2. date_iso (ascending)
#
# Input  : Escapement_PlotPipeline (SQLite table)
# Output : Escapement_PlotPipeline (rewritten)
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🔄 Step 40 (v5 Cleanup): Reordering rows inside the database...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD TABLE FROM DB
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
# VALIDATE REQUIRED COLUMNS
# ------------------------------------------------------------
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "date_iso", "by_adult5"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["by_adult5"] = pd.to_numeric(df["by_adult5"], errors="coerce").fillna(0).astype(int)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# ------------------------------------------------------------
# REORDER LOGIC
# ------------------------------------------------------------
def reorder_within_group(g):
    """Sort by biological year then by chronological date."""
    return g.sort_values(["by_adult5", "date_iso"]).reset_index(drop=True)

df = (
    df.groupby(group_cols, group_keys=False)
      .apply(reorder_within_group)
      .reset_index(drop=True)
)

# ------------------------------------------------------------
# SAVE BACK TO DATABASE
# ------------------------------------------------------------
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup v5 reorder complete!")
print(f"📊 Dataset reordered inside the DB by biological identity + by_adult5.")
print(f"🔢 Final row count: {len(df):,}")
print("🏁 Reordering finished successfully.")