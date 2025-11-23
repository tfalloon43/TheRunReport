# 2_species_plot.py
# ------------------------------------------------------------
# Step 2: Create Species_Plot column for simplified naming.
#
# Logic:
#   - Species_Plot = species_name (copy)
#   - If species_name ends with " Adult", remove that suffix.
#
# Input  : 100_Data/columbiadaily_raw.csv
# Output : 100_Data/2_species_plot.csv
#          (and updates columbiadaily_raw.csv in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🐟 Step 2: Creating Species_Plot column…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "columbiadaily_raw.csv"
output_path  = data_dir / "2_species_plot.csv"
recent_path  = data_dir / "columbiadaily_raw.csv"   # overwrite original

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from columbiadaily_raw.csv")

# ------------------------------------------------------------
# Validate required column
# ------------------------------------------------------------
if "species_name" not in df.columns:
    raise ValueError("❌ Column 'species_name' is missing in columbiadaily_raw.csv")

# ------------------------------------------------------------
# Build Species_Plot column
# ------------------------------------------------------------

def clean_species_name(name: str) -> str:
    """Remove ' Adult' if it appears at the end of the species_name."""
    if not isinstance(name, str):
        return name
    name = name.strip()
    if name.endswith(" Adult"):
        return name[:-6].rstrip()
    return name

df["Species_Plot"] = df["species_name"].apply(clean_species_name)

print("✅ Species_Plot column created successfully")

# ------------------------------------------------------------
# Save both outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Saved clean species file → {output_path}")
print(f"🔄 Updated columbiadaily_raw.csv in place")
print("🎯 Step 2 complete.")