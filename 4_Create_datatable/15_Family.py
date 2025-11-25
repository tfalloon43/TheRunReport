# 15_Family.py
# ------------------------------------------------------------
# Build 'Family' column from 'species' using family_map in lookup_maps
# Logic:
#   • For each row, look up its species (case-insensitive) in family_map.
#   • Write the corresponding family value.
#   • If species is blank or not found in the map, leave blank.
# Input  : 100_Data/csv_recent.csv
# Output : 100_Data/15_Family_output.csv + updated csv_recent.csv
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
# └── lookup_maps.py (project root)
# ------------------------------------------------------------

project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

# 👇 Add project root to Python path so lookup_maps can be found
project_root_str = str(project_root.resolve())
if project_root_str not in sys.path:
    sys.path.append(project_root_str)

from lookup_maps import family_map  # type: ignore

input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "15_Family_output.csv"
recent_path = data_dir / "csv_recent.csv"

print("🏗️  Step 15: Assigning Family from species...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun Step 14 first.")
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
