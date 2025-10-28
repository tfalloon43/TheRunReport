# 7_remove_singles.py
# ------------------------------------------------------------
# Step 7: Remove single-entry biological years
#
# Rules:
# - For each combination of:
#     facility, species, Stock, Stock_BO, bio_year
#   → If that group has only one row, remove it.
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/7_remove_singles_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 7: Removing single-entry bio_year groups...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "7_remove_singles_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Check columns
# ------------------------------------------------------------
required_cols = ["facility", "species", "Stock", "Stock_BO", "bio_year"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Identify and remove single-entry bio_year groups
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO", "bio_year"]

# Count how many rows per biological year group
counts = df.groupby(group_cols).size().reset_index(name="row_count")

# Merge counts back to dataframe
df = df.merge(counts, on=group_cols, how="left")

# Keep only groups with 2+ rows
before = len(df)
df = df[df["row_count"] > 1].drop(columns=["row_count"])
after = len(df)

removed = before - after
pct = round((removed / before * 100), 2)  # ✅ fixed: use built-in round()

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Removed {removed:,} single-entry rows ({pct}% of total)")
print(f"✅ Cleaned data saved → {output_path}")
print(f"🔄 csv_plotdata.csv updated with singles removed")