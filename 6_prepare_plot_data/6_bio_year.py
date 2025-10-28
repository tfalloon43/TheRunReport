# 6_bio_year.py
# ------------------------------------------------------------
# Step 6: Assign bio_year (biological year) IDs
#
# Rules:
# - Reset bio_year to 1 when any of these change:
#     facility, species, Stock, Stock_BO
# - bio_year increments (+1) when:
#     1) adult_diff < 0  → reset in counts
#     2) day_diff  > 40 → long time gap (new run)
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/6_bio_year_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 6: Assigning biological year (bio_year)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "6_bio_year_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Ensure columns exist
# ------------------------------------------------------------
required_cols = ["facility", "species", "Stock", "Stock_BO", "day_diff", "adult_diff"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

df["day_diff"] = pd.to_numeric(df["day_diff"], errors="coerce").fillna(0)
df["adult_diff"] = pd.to_numeric(df["adult_diff"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Sort for stability
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ------------------------------------------------------------
# Assign bio_year
# ------------------------------------------------------------
bio_years = []
current_year = 1
prev_keys = None

for i, row in df.iterrows():
    keys = tuple(row[col] for col in group_cols)
    day_diff = row["day_diff"]
    adult_diff = row["adult_diff"]

    # New group → reset to bio_year = 1
    if keys != prev_keys:
        current_year = 1
        prev_keys = keys

    # New biological year if adult_diff < 0 OR day_diff > 40
    elif adult_diff < 0 or day_diff > 40:
        current_year += 1

    bio_years.append(current_year)

df["bio_year"] = bio_years

# ------------------------------------------------------------
# Save output
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
max_bio = int(df["bio_year"].max())
unique_groups = len(df[group_cols].drop_duplicates())
print(f"✅ Biological year assignment complete → {output_path}")
print(f"📊 Max bio_year ID: {max_bio}")
print(f"📈 Across {unique_groups} unique facility/species/stock groups.")
