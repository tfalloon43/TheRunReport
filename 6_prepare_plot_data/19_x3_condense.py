# 19_x3_condense.py
# ------------------------------------------------------------
# Step 19 (v3): Condense x_count3 clusters regardless of by_adult3.
#
# Rules:
#   - Work within facility/species/Stock/Stock_BO groups
#   - Ignore by_adult3 boundaries
#   - For x_count3 >= 3:
#       → Group consecutive rows that share the same x_count3 value
#       → If all Adult_Total values are within ±10% of each other,
#         keep only the row with the largest Adult_Total
#         (remove all others)
#   - Rows with x_count3 < 3 remain unchanged.
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/19_x3_condense_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 19 (v3): Condensing x_count3 clusters (keep largest Adult_Total, ±10% tolerance)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "19_x3_condense_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
required_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total", "x_count3"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Type conversion
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
df["x_count3"] = pd.to_numeric(df["x_count3"], errors="coerce").fillna(0).astype(int)

# ------------------------------------------------------------
# Define logic
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]
TOLERANCE = 0.10  # ±10% Adult_Total tolerance

def condense_x_groups(g):
    """Condense contiguous x_count3 clusters regardless of by_adult3."""
    g = g.sort_values("date_iso").reset_index(drop=True)
    n = len(g)
    drop_idx = set()
    i = 0

    while i < n:
        count_val = g.loc[i, "x_count3"]

        if count_val >= 3:
            j = i
            # Find contiguous block with the same x_count3
            while j < n and g.loc[j, "x_count3"] == count_val:
                j += 1

            cluster = g.iloc[i:j]
            max_val = cluster["Adult_Total"].max()
            min_val = cluster["Adult_Total"].min()

            # If all Adult_Total values are within ±10%, keep the largest Adult_Total
            if max_val > 0 and ((max_val - min_val) / max_val) <= TOLERANCE:
                keep_idx = cluster.loc[cluster["Adult_Total"].idxmax()].name
                drop_idx.update(cluster.index.difference([keep_idx]))

            i = j  # move to next block
        else:
            i += 1

    return g.drop(index=drop_idx)

# ------------------------------------------------------------
# Apply per biological identity
# ------------------------------------------------------------
before_len = len(df)
df = df.groupby(group_cols, group_keys=False).apply(condense_x_groups).reset_index(drop=True)
after_len = len(df)
removed = before_len - after_len

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Condensing complete → {output_path}")
print(f"🧹 Removed {removed:,} duplicate rows across x_count3 clusters (kept largest Adult_Total, ±10% tolerance).")
print(f"📊 Final dataset: {after_len:,} rows remaining.")