# 14_species.py
# ------------------------------------------------------------
# Build 'species' column using species_headers from lookup_maps
# Logic:
#   • Iterate through 100_Data/csv_recent.csv in order.
#   • When a text_line matches (case-insensitive) one of the species_headers,
#       set that as the current species.
#   • All subsequent rows inherit that species until a new match appears.
#   • The species column gets the exact string from species_headers (case preserved).
#   • Rows before the first header remain blank.
# Input  : 100_Data/csv_recent.csv
# Output : 100_Data/14_species_output.csv + csv_recent.csv
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

from lookup_maps import species_headers  # type: ignore

input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "14_species_output.csv"
recent_path = data_dir / "csv_recent.csv"

print("🏗️  Step 14: Assigning species from species_headers...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun Step 13 first.")
df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Main logic
# ------------------------------------------------------------
# Normalize comparison set (case-insensitive)
species_lookup = {s.lower(): s for s in species_headers}

current_species = ""
species_list = []

for _, row in df.iterrows():
    text_val = str(row.get("text_line", "")).strip()

    # Check if this line is a species header (case-insensitive)
    key = text_val.lower()
    if key in species_lookup:
        current_species = species_lookup[key]  # exact-cased version
        species_list.append(current_species)
    else:
        # Carry forward the last seen species (if any)
        species_list.append(current_species if current_species else "")

df["species"] = species_list

# ------------------------------------------------------------
# Save outputs (in 100_Data)
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
filled = df["species"].astype(str).str.strip().ne("").sum()
print(f"✅ Species assignment complete → {output_path}")
print(f"🔄 csv_recent.csv updated with species column")
print(f"📊 {filled} rows now have a species value.")
