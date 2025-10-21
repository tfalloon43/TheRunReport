# 3_hatchery_blank.py
# ------------------------------------------------------------
# Step 3 of Data Clean Pipeline
# Removes rows where 'Hatchery_Name' is blank or missing.
# Input  : 100_Data/csv_reduce.csv
# Output : 100_Data/3_hatchery_blank_output.csv + updated csv_reduce.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 3: Removing rows with blank Hatchery_Name...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_reduce.csv"
output_path = data_dir / "3_hatchery_blank_output.csv"
recent_path = data_dir / "csv_reduce.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Remove rows where Hatchery_Name is blank, NaN, or whitespace
# ------------------------------------------------------------
if "Hatchery_Name" not in df.columns:
    raise ValueError("❌ Missing column: 'Hatchery_Name'")

before_count = len(df)

df = df[df["Hatchery_Name"].astype(str).str.strip().ne("") & df["Hatchery_Name"].notna()]
df = df.reset_index(drop=True)

after_count = len(df)
removed = before_count - after_count

print(f"🧹 Removed {removed:,} rows with blank Hatchery_Name.")
print(f"✅ {after_count:,} rows remain.")

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Output saved → {output_path}")
print(f"🔄 csv_reduce.csv updated for next cleaning step.")
