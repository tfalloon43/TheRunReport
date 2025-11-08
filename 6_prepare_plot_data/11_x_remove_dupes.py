# 11_x_remove_dupes.py
# ------------------------------------------------------------
# Step 11: Remove duplicate Adult_Total values within flagged
#           short-run clusters (x_count ≥ 3)
#
# Rules:
#   - Always work within biological identity:
#       facility, species, Stock, Stock_BO
#   - If x_count ≤ 2 → leave row unchanged
#   - If x_count ≥ 3 → group consecutive rows with same x_count
#       • Within that cluster, look for duplicate Adult_Total
#       • Keep only the row with the oldest (earliest) date_iso
#         for each duplicated Adult_Total
#   - Output retains all unflagged rows and cleaned clusters
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/11_x_remove_dupes_output.csv
#          + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 11: Removing duplicate Adult_Total rows within x_count ≥ 3 clusters...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "11_x_remove_dupes_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Verify required columns
# ------------------------------------------------------------
required_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "x_count", "Adult_Total"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Ensure proper datatypes
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["x_count"] = pd.to_numeric(df["x_count"], errors="coerce").fillna(0).astype(int)
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Core logic per biological identity
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]

def process_group(g):
    g = g.sort_values("date_iso").reset_index(drop=True)
    n = len(g)
    keep_mask = [True] * n  # start with all rows kept

    i = 0
    while i < n:
        xval = g.loc[i, "x_count"]
        if xval < 3:
            i += 1
            continue

        # Find the extent of this contiguous cluster with same x_count
        j = i
        while j < n and g.loc[j, "x_count"] == xval:
            j += 1

        cluster = g.loc[i:j].copy()

        # Remove duplicate Adult_Total, keeping oldest date
        dupes = cluster.duplicated(subset=["Adult_Total"], keep="first")
        for idx in cluster.index[dupes]:
            keep_mask[idx] = False

        i = j  # move past this cluster

    return g.loc[keep_mask]

# ------------------------------------------------------------
# Apply per biological group
# ------------------------------------------------------------
before = len(df)
cleaned = df.groupby(group_cols, group_keys=False).apply(process_group).reset_index(drop=True)
after = len(cleaned)
removed = before - after

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
cleaned.to_csv(output_path, index=False)
cleaned.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Duplicate cleanup complete → {output_path}")
print(f"🧹 Removed {removed:,} duplicate rows (x_count ≥ 3 clusters only).")
print(f"📊 Final dataset: {after:,} rows.")