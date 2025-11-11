# 4_hatchfamily.py
# ------------------------------------------------------------
# Step 4: Create combined hatchery + family identifier.
#
# Logic:
#   hatchfamily = facility + " - " + Family
#
# Input  : 100_Data/csv_10av.csv
# Output : 100_Data/4_hatchfamily_output.csv + updated csv_10av.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 4: Creating hatchfamily column (facility + Family)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_10av.csv"
output_path  = data_dir / "4_hatchfamily_output.csv"
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
required_cols = ["facility", "Family"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Create hatchfamily column
# ------------------------------------------------------------
df["facility"] = df["facility"].astype(str).str.strip()
df["Family"]   = df["Family"].astype(str).str.strip()
df["hatchfamily"] = df["facility"] + " - " + df["Family"]

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"✅ hatchfamily column created successfully → {output_path}")
print(f"🔄 csv_10av.csv updated with new hatchfamily column")