# 21_x4_condense.py
# ------------------------------------------------------------
# Step 21 (v4): Condense x_count4 clusters using midpoint (min/max) logic.
#
# Rules:
#   - Work within facility/species/Stock/Stock_BO groups
#   - If x_count4 < 3 → leave rows unchanged
#   - For x_count4 >= 3:
#       → Group contiguous rows with same x_count4
#       → If Adult_Total is ascending → leave unchanged
#       → Else:
#           - Compute midpoint = (min + max) / 2
#           - "Large" = Adult_Total > midpoint
#           - "Small" = Adult_Total ≤ midpoint
#           - Keep:
#               * All "small" rows (sorted by date)
#               * Only one "large" row (earliest date)
#           - Remove the other large rows
#           - Move the one remaining large value to the first row of the cluster.
#             If it wasn’t already first, set its original Adult_Total to 0.
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/21_x4_condense_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 21 (v4): Condensing x_count4 clusters (midpoint min/max logic)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "21_x4_condense_output.csv"
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
required_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total", "x_count4"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Type conversion
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
df["x_count4"] = pd.to_numeric(df["x_count4"], errors="coerce").fillna(0).astype(int)

# ------------------------------------------------------------
# Condense logic
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]

def condense_x4_midpoint(g):
    """Condense contiguous x_count4 clusters using midpoint min/max rule."""
    g = g.sort_values("date_iso").reset_index(drop=True)
    n = len(g)
    drop_idx = set()
    i = 0

    while i < n:
        count_val = g.loc[i, "x_count4"]

        if count_val < 3:
            i += 1
            continue

        # Find contiguous block with same x_count4
        j = i
        while j < n and g.loc[j, "x_count4"] == count_val:
            j += 1

        cluster = g.iloc[i:j]
        adult_vals = cluster["Adult_Total"].tolist()

        # If strictly ascending, leave unchanged
        if adult_vals == sorted(adult_vals):
            i = j
            continue

        # Compute midpoint
        min_val = cluster["Adult_Total"].min()
        max_val = cluster["Adult_Total"].max()
        midpoint = (min_val + max_val) / 2.0

        # Label large/small
        large_mask = cluster["Adult_Total"] > midpoint
        small_mask = ~large_mask

        large_rows = cluster[large_mask]
        small_rows = cluster[small_mask]

        if not large_rows.empty:
            # Keep only earliest large
            earliest_large = large_rows.sort_values("date_iso").iloc[0]
            keep_large_idx = earliest_large.name

            # Drop other large rows
            drop_idx.update(large_rows.index.difference([keep_large_idx]))

            # Move the remaining large value to top of cluster
            first_idx = cluster.index[0]
            if first_idx != keep_large_idx:
                g.loc[first_idx, "Adult_Total"] = earliest_large["Adult_Total"]
                g.loc[keep_large_idx, "Adult_Total"] = 0  # zero out original

        i = j

    return g.drop(index=drop_idx)

# ------------------------------------------------------------
# Apply per biological identity
# ------------------------------------------------------------
before_len = len(df)
df = df.groupby(group_cols, group_keys=False).apply(condense_x4_midpoint).reset_index(drop=True)
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
print(f"🧹 Removed {removed:,} rows across x_count4 clusters (midpoint min/max rule).")
print(f"📊 Final dataset: {after_len:,} rows remaining.")