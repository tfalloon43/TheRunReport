# 8_bio_length.py
# ------------------------------------------------------------
# Step 8: Add bio_length (number of rows in each biological year)
#
# Rules:
# - For each combination of:
#     facility, species, Stock, Stock_BO, bio_year
#   → Count how many rows belong to that bio_year
# - Assign that count as `bio_length` for every row in that group
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/8_bio_length_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 8: Calculating bio_length for each biological year...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "8_bio_length_output.csv"
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
# Calculate bio_length
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO", "bio_year"]

# Count how many rows in each group
bio_lengths = df.groupby(group_cols).size().reset_index(name="bio_length")

# Merge back into main dataframe
df = df.merge(bio_lengths, on=group_cols, how="left")

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
unique_bio_years = len(df[group_cols].drop_duplicates())
avg_len = df["bio_length"].mean().round(2)
max_len = df["bio_length"].max()

print(f"✅ Added bio_length column for {unique_bio_years:,} bio_year groups")
print(f"📊 Average bio_length: {avg_len}")
print(f"📈 Longest bio_year length: {max_len}")
print(f"💾 Saved to {output_path}")
print(f"🔄 csv_plotdata.csv updated")