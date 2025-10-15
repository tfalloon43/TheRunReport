# 13_species.py
# ------------------------------------------------------------
# Build 'species' column using species_headers from lookup_maps
# Logic:
#   • Iterate through csv_recent.csv in order.
#   • When a text_line matches (case-insensitive) one of the species_headers,
#       set that as the current species.
#   • All subsequent rows inherit that species until a new match appears.
#   • The species column gets the exact string from species_headers (case preserved).
#   • Rows before the first header remain blank.
# Input  : csv_recent.csv
# Output : 13_species_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import os
import sys
import pandas as pd

# --- Fix import path so we can access lookup_maps.py ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lookup_maps import species_headers  # ✅ list of species header strings

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "13_species_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 13: Assigning species from species_headers...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")

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

    # check if this line is a species header (case-insensitive)
    key = text_val.lower()
    if key in species_lookup:
        current_species = species_lookup[key]  # exact-cased version
        species_list.append(current_species)
    else:
        # carry forward the last seen species (if any)
        species_list.append(current_species if current_species else "")

df["species"] = species_list

# ------------------------------------------------------------
# Save outputs
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