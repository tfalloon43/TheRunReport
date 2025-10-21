# 18_basin.py
# ------------------------------------------------------------
# Step 18 of Create Datatable Pipeline
# Builds the 'basin' column using basin_map from lookup_maps.py.
# Logic:
#   • Looks up each Hatchery_Name (case-insensitive) in basin_map.
#   • If found → assign corresponding basin name.
#   • If not found or Hatchery_Name is blank → leave basin blank.
# Input  : 100_Data/csv_recent.csv
# Output : 100_Data/18_basin_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
import sys
import os

print("🏗️ Step 18: Assigning basin from Hatchery_Name...")

# ------------------------------------------------------------
# Setup paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

# ✅ Ensure lookup_maps.py can be imported
data_path = str(data_dir.resolve())
if data_path not in sys.path:
    sys.path.insert(0, data_path)

# Import basin_map
try:
    from lookup_maps import basin_map  # type: ignore
    print("✅ Successfully imported basin_map from lookup_maps.py")
except ModuleNotFoundError as e:
    print("❌ Could not import lookup_maps.py — ensure it’s inside 100_Data/")
    raise e

# File paths
input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "18_basin_output.csv"
recent_path = data_dir / "csv_recent.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing {input_path} — run previous step first.")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Build basin column
# ------------------------------------------------------------
if "Hatchery_Name" not in df.columns:
    raise ValueError("❌ Missing 'Hatchery_Name' column in input file.")

def map_basin(hatchery_name):
    """Lookup basin based on Hatchery_Name (case-insensitive)."""
    if not isinstance(hatchery_name, str) or not hatchery_name.strip():
        return ""
    key = hatchery_name.strip().upper()
    return basin_map.get(key, "").strip()

df["basin"] = df["Hatchery_Name"].apply(map_basin)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
filled = df["basin"].astype(str).str.strip().ne("").sum()
total = len(df)
print(f"✅ Basin mapping complete → {output_path}")
print(f"🔄 csv_recent.csv updated with basin column")
print(f"📊 {filled} of {total} rows populated with basin names.")
