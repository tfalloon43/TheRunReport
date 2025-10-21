# 14_Family.py
# ------------------------------------------------------------
# Build 'Family' column from 'species' using family_map in lookup_maps
# Logic:
#   • For each row, look up its species (case-insensitive) in family_map.
#   • Write the corresponding family value.
#   • If species is blank or not found in the map, leave blank.
# Input  : 100_Data/csv_recent.csv
# Output : 100_Data/14_Family_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import pandas as pd
import sys
from pathlib import Path
import os

# ------------------------------------------------------------
# Setup imports and paths
# ------------------------------------------------------------
# Project structure:
# TheRunReport/
# ├── 4_Create_datatable/
# └── 100_Data/lookup_maps.py
# ------------------------------------------------------------

project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

# 👇 Add 100_Data folder to Python path so lookup_maps can be found
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
data_path = os.path.join(project_root, "100_Data")
sys.path.append(data_path)

from lookup_maps import family_map  # type: ignore

input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "14_Family_output.csv"
recent_path = data_dir / "csv_recent.csv"

print("🏗️  Step 14: Assigning Family from species...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun Step 13 first.")
df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Main logic
# ------------------------------------------------------------
def map_family(species):
    if not isinstance(species, str) or not species.strip():
        return ""
    lower_species = species.strip().lower()
    return family_map.get(lower_species, "")

df["Family"] = df["species"].apply(map_family)

# ------------------------------------------------------------
# Save outputs (in 100_Data)
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
filled = df["Family"].astype(str).str.strip().ne("").sum()
print(f"✅ Family assignment complete → {output_path}")
print(f"🔄 csv_recent.csv updated with Family column")
print(f"📊 {filled} rows now have a Family value.")