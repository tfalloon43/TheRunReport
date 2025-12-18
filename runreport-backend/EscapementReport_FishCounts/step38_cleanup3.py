# step38_cleanup3.py
# ------------------------------------------------------------
# Step 38 (v3): Condense x_count3 clusters using max/2 logic
#
# Rules:
#   - Work within facility/species/Stock/Stock_BO groups
#   - If x_count3 < 3 → keep all rows
#   - For x_count3 >= 3:
#       → Identify contiguous x_count3 blocks
#       → threshold = max(Adult_Total) / 2
#       → "large" rows: Adult_Total > threshold
#       → "small" rows: Adult_Total ≤ threshold
#       → Keep:
#           * ALL small rows
#           * ONLY earliest-date large row
#       → Drop all other large rows
#
# IMPORTANT:
#   • Full rows are kept/dropped (no column mutation)
#   • No fabricated values
#   • No date modification
#
# DB input/output: Escapement_PlotPipeline
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 38: Condensing x_count3 clusters (max/2 logic, row-safe)...")

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
# REQUIRED COLUMNS
# ------------------------------------------------------------
required = [
    "facility", "species", "Stock", "Stock_BO",
    "date_iso", "Adult_Total",
    "x_count3"
]

missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# TYPE NORMALIZATION (READ-ONLY)
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
df["x_count3"] = pd.to_numeric(df["x_count3"], errors="coerce").fillna(0).astype(int)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# ------------------------------------------------------------
# CORE LOGIC
# ------------------------------------------------------------
def condense_x3(g: pd.DataFrame) -> pd.DataFrame:
    """
    Condense contiguous x_count3 clusters using max/2 logic.
    Full rows are selected/dropped — no mutation.
    """
    g = g.sort_values("date_iso").reset_index(drop=True)

    keep_indices = []
    n = len(g)
    i = 0

    while i < n:
        count_val = g.loc[i, "x_count3"]

        # Keep untouched if not a cluster
        if count_val < 3:
            keep_indices.append(g.index[i])
            i += 1
            continue

        # Find contiguous cluster
        j = i
        while j < n and g.loc[j, "x_count3"] == count_val:
            j += 1

        cluster = g.iloc[i:j]

        max_val = cluster["Adult_Total"].max()
        threshold = max_val / 2.0

        large = cluster[cluster["Adult_Total"] > threshold]
        small = cluster[cluster["Adult_Total"] <= threshold]

        # Keep ALL small rows
        keep_indices.extend(small.index.tolist())

        # Keep earliest large row (if any)
        if not large.empty:
            earliest_large_idx = (
                large.sort_values("date_iso").index[0]
            )
            keep_indices.append(earliest_large_idx)

        i = j

    return g.loc[sorted(set(keep_indices))]


# ------------------------------------------------------------
# APPLY PER BIOLOGICAL IDENTITY
# ------------------------------------------------------------
before = len(df)

df_out = (
    df.groupby(group_cols, group_keys=False)
      .apply(condense_x3)
      .reset_index(drop=True)
)

after = len(df_out)
removed = before - after

# ------------------------------------------------------------
# WRITE BACK TO DB
# ------------------------------------------------------------
print("💾 Writing condensed results back to Escapement_PlotPipeline...")
df_out.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup 3 Complete!")
print(f"🧹 Rows removed: {removed:,}")
print(f"📊 Final rows: {after:,}")
print("🏁 x_count3 clusters safely condensed (row-preserving).")