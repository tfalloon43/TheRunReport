# step38_cleanup4.py
# ------------------------------------------------------------
# Step 38 (v4): Condense x_count4 clusters using midpoint min/max logic.
#
# Rules:
#   - Work within facility/species/Stock/Stock_BO groups
#   - If x_count4 < 3 → leave unchanged
#   - For x_count4 >= 3:
#       → Identify contiguous blocks with same x_count4
#       → If Adult Total is strictly ascending → leave unchanged
#       → Else:
#           - midpoint = (min + max) / 2
#           - "large" rows: Adult Total > midpoint
#           - "small" rows: Adult Total ≤ midpoint
#           - Keep:
#               * All "small" rows
#               * Only earliest-date "large" row
#           - Move the kept large value to top of cluster
#             and zero out its original location if moved.
#
# DB input/output: Escapement_PlotPipeline
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 38: Condensing x_count4 clusters (midpoint logic)...")

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

# ------------------------------------------------------------
# REQUIRED COLUMNS
# ------------------------------------------------------------
required = [
    "facility", "species", "Stock", "Stock_BO",
    "date_iso", "Adult Total", "x_count4"
]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult Total"] = pd.to_numeric(df["Adult Total"], errors="coerce").fillna(0)
df["x_count4"] = pd.to_numeric(df["x_count4"], errors="coerce").fillna(0).astype(int)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# ------------------------------------------------------------
# MAIN LOGIC
# ------------------------------------------------------------
def condense_x4(g):
    """Apply midpoint compression to contiguous x_count4 clusters."""
    g = g.sort_values("date_iso").reset_index(drop=True)
    drop_idx = set()
    n = len(g)
    i = 0

    while i < n:
        count_val = g.loc[i, "x_count4"]

        # Only operate on clusters with x_count4 >= 3
        if count_val < 3:
            i += 1
            continue

        # Find contiguous block with same x_count4
        j = i
        while j < n and g.loc[j, "x_count4"] == count_val:
            j += 1

        cluster = g.iloc[i:j]
        adult_vals = cluster["Adult Total"].tolist()

        # If strictly ascending, no modification
        if adult_vals == sorted(adult_vals):
            i = j
            continue

        # Compute midpoint
        min_val = cluster["Adult Total"].min()
        max_val = cluster["Adult Total"].max()
        midpoint = (min_val + max_val) / 2.0

        # Identify large and small rows
        is_large = cluster["Adult Total"] > midpoint
        large_rows = cluster[is_large]
        small_rows = cluster[~is_large]

        if not large_rows.empty:
            # Keep earliest large row
            earliest_large = large_rows.sort_values("date_iso").iloc[0]
            keep_large_idx = earliest_large.name

            # Drop all other large rows
            drop_idx.update(large_rows.index.difference([keep_large_idx]))

            # Move the large Adult Total to top of cluster
            first_idx = cluster.index[0]

            if first_idx != keep_large_idx:
                # assign to first row
                g.loc[first_idx, "Adult Total"] = earliest_large["Adult Total"]
                # zero out original value
                g.loc[keep_large_idx, "Adult Total"] = 0

        i = j

    return g.drop(index=drop_idx)


# ------------------------------------------------------------
# APPLY PER BIOLOGICAL IDENTITY
# ------------------------------------------------------------
before = len(df)
df = df.groupby(group_cols, group_keys=False).apply(condense_x4).reset_index(drop=True)
after = len(df)
removed = before - after

# ------------------------------------------------------------
# WRITE BACK TO DB
# ------------------------------------------------------------
print("💾 Writing condensed results back to Escapement_PlotPipeline...")
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup 4 Complete!")
print(f"🧹 Rows removed: {removed:,}")
print(f"📊 Final rows: {after:,}")
print("🏁 Midpoint min/max condensing successfully applied.")