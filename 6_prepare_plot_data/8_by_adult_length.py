# 8_by_adult_length.py
# ------------------------------------------------------------
# Step 8: Compute by_adult_length — number of rows in each
# biological year based on adult behavior (by_adult)
#
# Rules:
# - Group by facility, species, Stock, Stock_BO, and by_adult
# - Count number of rows in each group
# - Assign that count to every row as by_adult_length
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/8_by_adult_length_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 8: Calculating by_adult_length for each biological year...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "8_by_adult_length_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Ensure required columns exist
# ------------------------------------------------------------
required_cols = ["facility", "species", "Stock", "Stock_BO", "by_adult"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Compute by_adult_length
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO", "by_adult"]

# Count how many rows per by_adult
lengths = df.groupby(group_cols).size().reset_index(name="by_adult_length")

# Merge back into main dataframe
df = df.merge(lengths, on=group_cols, how="left")

# ------------------------------------------------------------
# Save output
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
mean_len = df["by_adult_length"].mean().round(2)
max_len = int(df["by_adult_length"].max())
unique_runs = len(df[group_cols].drop_duplicates())

print(f"✅ by_adult_length calculation complete → {output_path}")
print(f"📊 Avg length per by_adult: {mean_len}")
print(f"📈 Max by_adult_length: {max_len}")
print(f"🔢 Across {unique_runs:,} unique facility/species/stock/year groups.")