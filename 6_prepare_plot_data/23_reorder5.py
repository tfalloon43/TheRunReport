# 23_reorder5.py
# ------------------------------------------------------------
# Step 23 (v5): Reorder dataset by biological identity and biological year.
#
# Rules:
#   - Work within facility/species/Stock/Stock_BO groups
#   - Group rows by by_adult5 (biological year)
#   - Within each biological year, sort by date_iso (ascending)
#
# Purpose:
#   Ensures all rows within each biological identity and biological year
#   are in true chronological order before plotting, exporting, or
#   running subsequent metrics.
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/23_reorder5_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🔄 Step 23 (v5): Reordering rows by biological identity and biological year...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "23_reorder5_output.csv"
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
    "date_iso", "by_adult5"
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Normalize types
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["by_adult5"] = pd.to_numeric(df["by_adult5"], errors="coerce").fillna(0).astype(int)

# ------------------------------------------------------------
# Sort logic
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]

def reorder_within_group(g):
    """Order rows by biological year and then by date."""
    return g.sort_values(["by_adult5", "date_iso"]).reset_index(drop=True)

# Apply per biological identity
before_order = df.copy()
df = (
    df.groupby(group_cols, group_keys=False)
      .apply(reorder_within_group)
      .reset_index(drop=True)
)

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Reordering complete → {output_path}")
print(f"📊 Dataset reordered by biological identity and by_adult5 (chronological).")
print(f"🔢 Total rows: {len(df):,}")
