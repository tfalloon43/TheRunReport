# 17_x2_condense.py
# ------------------------------------------------------------
# Step 17: Condense similar X clusters (x_count2 ≥ 3)
# within the same biological identity group.
#
# Rules:
#   - Work within facility/species/Stock/Stock_BO groups
#   - If x_count2 is 0, 1, or 2 → leave unchanged
#   - For x_count2 ≥ 3:
#       → Group consecutive rows with the same x_count2
#       → If all Adult_Total values are within ±2% of each other,
#         keep only the oldest date_iso (remove newer ones)
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/17_x2_condense_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 17: Condensing similar X clusters (x_count2 ≥ 3, within ±2% Adult_Total)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "17_x2_condense_output.csv"
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
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "date_iso", "x_count2", "Adult_Total"
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Type conversion
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
df["x_count2"] = pd.to_numeric(df["x_count2"], errors="coerce").fillna(0).astype(int)

# ------------------------------------------------------------
# Define processing logic
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]

def condense_x_groups(g):
    """Condense clusters of rows with x_count2 >= 3 and similar Adult_Total values."""
    g = g.sort_values("date_iso").reset_index(drop=True)
    n = len(g)
    drop_idx = set()
    i = 0

    while i < n:
        curr_count = g.loc[i, "x_count2"]

        # Only consider x_count2 >= 3
        if curr_count >= 3:
            j = i
            # Find consecutive rows with the same x_count2
            while j < n and g.loc[j, "x_count2"] == curr_count:
                j += 1

            cluster = g.iloc[i:j]

            # Check if Adult_Total values are within ±2%
            max_val = cluster["Adult_Total"].max()
            min_val = cluster["Adult_Total"].min()
            if max_val > 0 and ((max_val - min_val) / max_val) <= 0.02:
                # Drop all except oldest date_iso
                oldest_idx = cluster["date_iso"].idxmin()
                cluster_to_drop = cluster.index.difference([oldest_idx])
                drop_idx.update(cluster_to_drop)

            i = j  # skip to next segment
        else:
            i += 1

    # Remove duplicates (drop marked indices)
    g = g.drop(index=drop_idx)
    return g

# ------------------------------------------------------------
# Apply per biological identity group
# ------------------------------------------------------------
before_len = len(df)
df = df.groupby(group_cols, group_keys=False).apply(condense_x_groups).reset_index(drop=True)
after_len = len(df)
removed_rows = before_len - after_len

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Condensing complete → {output_path}")
print(f"🧹 Removed {removed_rows:,} near-duplicate rows (within ±2% Adult_Total, oldest kept).")
print(f"📊 Final dataset: {after_len:,} rows remaining.")