# 14_Family.py
# ------------------------------------------------------------
# Build 'Family' column from 'species' using family_map in lookup_maps
# Logic:
#   • For each row, look up its species (case-insensitive) in family_map.
#   • Write the corresponding family value.
#   • If species is blank or not found in the map, leave blank.
# Input  : csv_recent.csv
# Output : 14_Family_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import os
import sys
import pandas as pd

# --- Fix import path so we can access lookup_maps.py ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lookup_maps import family_map  # ✅ dictionary of species → family

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "14_Family_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 14: Assigning Family from species...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")

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
# Save outputs
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