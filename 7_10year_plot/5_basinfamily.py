# 5_basinfamily.py
# ------------------------------------------------------------
# Step 5: Create combined basin + family identifier.
#
# Logic:
#   basinfamily = basin + " - " + Family
#
# Input  : 100_Data/csv_10av.csv
# Output : 100_Data/5_basinfamily_output.csv + updated csv_10av.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 5: Creating basinfamily column (basin + Family)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_10av.csv"
output_path  = data_dir / "5_basinfamily_output.csv"
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
required_cols = ["basin", "Family"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Create basinfamily column
# ------------------------------------------------------------
df["basin"]  = df["basin"].astype(str).str.strip()
df["Family"] = df["Family"].astype(str).str.strip()
df["basinfamily"] = df["basin"] + " - " + df["Family"]

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"✅ basinfamily column created successfully → {output_path}")
print(f"🔄 csv_10av.csv updated with new basinfamily column")
