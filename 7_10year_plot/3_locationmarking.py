# 3_locationmarking.py
# ------------------------------------------------------------
# Step 3: Create all location-based identifier columns
#
# Columns added:
#   • hatchspecies  = facility + " - " + species
#   • hatchfamily   = facility + " - " + Family
#   • basinfamily   = basin + " - " + Family
#   • basinspecies  = basin + " - " + species
#
# Input  : 100_Data/csv_10av.csv
# Output : 100_Data/3_locationmarking_output.csv + updated csv_10av.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 3: Creating location-based identifier columns (hatchspecies, hatchfamily, basinfamily, basinspecies)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_10av.csv"
output_path  = data_dir / "3_locationmarking_output.csv"
recent_path  = data_dir / "csv_10av.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun previous step first.")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
required_cols = ["facility", "basin", "species", "Family"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Normalize key columns (strip whitespace, ensure string type)
# ------------------------------------------------------------
for col in required_cols:
    df[col] = df[col].astype(str).str.strip()

# ------------------------------------------------------------
# Create combined identifier columns
# ------------------------------------------------------------
df["hatchspecies"] = df["facility"] + " - " + df["species"]
df["hatchfamily"]  = df["facility"] + " - " + df["Family"]
df["basinfamily"]  = df["basin"]    + " - " + df["Family"]
df["basinspecies"] = df["basin"]    + " - " + df["species"]

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"✅ All identifier columns created successfully → {output_path}")
print(f"🔄 csv_10av.csv updated with new columns:")
print("   • hatchspecies")
print("   • hatchfamily")
print("   • basinfamily")
print("   • basinspecies")
print("🎯 Step 3 complete.")