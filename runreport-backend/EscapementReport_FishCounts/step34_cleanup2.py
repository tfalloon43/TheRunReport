# step34_cleanup2.py
# ------------------------------------------------------------
# Step 34: Condense X clusters (iteration2 cleanup)
#
# Logic:
#   • Work inside DB table Escapement_PlotPipeline
#   • Only rows where x_count2 ≥ 3 are considered
#   • Identify consecutive same-x_count2 clusters
#   • If Adult Total values are all within ±2%
#       → keep only the oldest date_iso row
#       → remove all newer rows
#
# Output: Escapement_PlotPipeline rewritten in-place
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 34: Condensing X clusters (x_count2 ≥ 3, within ±2% Adult Total)...")

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
    "date_iso", "x_count2", "Adult_Total"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# TYPE NORMALIZATION
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
df["x_count2"] = pd.to_numeric(df["x_count2"], errors="coerce").fillna(0).astype(int)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# ------------------------------------------------------------
# CORE CLEANUP LOGIC
# ------------------------------------------------------------
def condense_x_clusters(g):
    """Condense clusters with x_count2 ≥ 3 and Adult_Total within ±2%."""
    g = g.sort_values("date_iso").reset_index(drop=True)
    n = len(g)
    drop_idx = set()
    i = 0

    while i < n:
        xc = g.loc[i, "x_count2"]

        if xc >= 3:
            j = i
            # Expand forward as long as x_count2 matches
            while j < n and g.loc[j, "x_count2"] == xc:
                j += 1

            cluster = g.iloc[i:j]
            max_val = cluster["Adult_Total"].max()
            min_val = cluster["Adult_Total"].min()

            # Check ±2% spread
            if max_val > 0 and ((max_val - min_val) / max_val) <= 0.02:
                oldest_idx = cluster["date_iso"].idxmin()
                # All except oldest get dropped
                drop_targets = cluster.index.difference([oldest_idx])
                drop_idx.update(drop_targets)

            i = j
        else:
            i += 1

    return g.drop(index=drop_idx)

# ------------------------------------------------------------
# APPLY PER GROUP
# ------------------------------------------------------------
before = len(df)

df_clean = (
    df.groupby(group_cols, group_keys=False)
      .apply(condense_x_clusters)
      .reset_index(drop=True)
)

after = len(df_clean)
removed = before - after

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df_clean.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup2 complete!")
print(f"🧹 Removed {removed:,} rows from x_count2 clusters.")
print(f"📊 Final row count: {after:,}")
print(f"📁 Escapement_PlotPipeline updated in database.")
