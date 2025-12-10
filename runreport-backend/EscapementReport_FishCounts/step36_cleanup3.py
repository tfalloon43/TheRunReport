# step36_cleanup3.py
# ------------------------------------------------------------
# Step 36: Cleanup 3 — Condense x_count3 clusters (DB version)
#
# Rules:
#   - Work inside Escapement_PlotPipeline (SQLite)
#   - Only consider x_count3 >= 3
#   - Within facility/species/Stock/Stock_BO:
#         → Group consecutive rows with identical x_count3
#         → If Adult Total values are within ±10%, keep only the
#           row with the **largest Adult Total**
#   - Write cleaned table back to DB
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 36: Condensing x_count3 clusters (±10% tolerance, keep largest Adult Total)...")

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

# ------------------------------------------------------------
# REQUIRED COLUMNS CHECK
# ------------------------------------------------------------
required_cols = [
    "facility",
    "species",
    "Stock",
    "Stock_BO",
    "date_iso",
    "Adult Total",
    "x_count3"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult Total"] = pd.to_numeric(df["Adult Total"], errors="coerce").fillna(0)
df["x_count3"] = pd.to_numeric(df["x_count3"], errors="coerce").fillna(0).astype(int)

TOLERANCE = 0.10  # ±10%

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# ------------------------------------------------------------
# CONDENSING LOGIC
# ------------------------------------------------------------
def condense_clusters(g):
    """Condense contiguous x_count3 clusters, keeping only the row with
    the largest Adult Total if within ±10% tolerance."""
    g = g.sort_values("date_iso").reset_index(drop=True)
    n = len(g)
    drop_idx = set()

    i = 0
    while i < n:
        curr = g.loc[i, "x_count3"]

        if curr >= 3:
            j = i
            # Find contiguous runs of x_count3
            while j < n and g.loc[j, "x_count3"] == curr:
                j += 1

            cluster = g.iloc[i:j]

            max_val = cluster["Adult Total"].max()
            min_val = cluster["Adult Total"].min()

            # If within ±10%, keep only the largest Adult Total
            if max_val > 0 and ((max_val - min_val) / max_val) <= TOLERANCE:
                keep_idx = cluster["Adult Total"].idxmax()
                drop_idx.update(cluster.index.difference([keep_idx]))

            i = j
        else:
            i += 1

    return g.drop(index=drop_idx)


# ------------------------------------------------------------
# APPLY PER GROUP
# ------------------------------------------------------------
before_len = len(df)

df_clean = (
    df.groupby(group_cols, group_keys=False)
      .apply(condense_clusters)
      .reset_index(drop=True)
)

after_len = len(df_clean)
removed = before_len - after_len

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
print("💾 Writing cleaned table back to Escapement_PlotPipeline...")
df_clean.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)

conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup 3 (x_count3 condense) Complete!")
print(f"🧹 Removed {removed:,} rows from x_count3 clusters.")
print(f"📊 Final dataset: {after_len:,} rows remain.")
print("🏁 Cleanup phase 3 successfully applied.")