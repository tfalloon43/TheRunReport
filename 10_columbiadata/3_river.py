# 3_river.py
# ------------------------------------------------------------
# Step 3: Add "river" column based on dam → river mapping.
#
# Uses Columbia_or_Snake mapping from lookup_maps.py
#
# Input  : 100_Data/columbiadaily_raw.csv
# Output : 100_Data/3_river.csv
#          (and updates columbiadaily_raw.csv in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
import sys

print("🌊 Step 3: Mapping dams to river systems…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "columbiadaily_raw.csv"
output_path  = data_dir / "3_river.csv"
recent_path  = data_dir / "columbiadaily_raw.csv"

lookup_path  = data_dir / "lookup_maps.py"

# ------------------------------------------------------------
# Load mapping from lookup_maps.py
# ------------------------------------------------------------
if not lookup_path.exists():
    raise FileNotFoundError(f"❌ lookup_maps.py not found at: {lookup_path}")

# Dynamically import lookup_maps.py
sys.path.append(str(data_dir))
try:
    import lookup_maps
except Exception as e:
    raise RuntimeError(f"❌ Could not import lookup_maps.py: {e}")

# Ensure the mapping exists
if not hasattr(lookup_maps, "Columbia_or_Snake"):
    raise KeyError("❌ lookup_maps.py does not define Columbia_or_Snake")

DAM_TO_RIVER = lookup_maps.Columbia_or_Snake
print("📘 Loaded dam → river lookup map from lookup_maps.py")

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
if "dam_code" not in df.columns:
    raise ValueError("❌ Column 'dam_code' is missing. Ensure 1_datapull.py created it.")

# ------------------------------------------------------------
# Map dam_code → river
# ------------------------------------------------------------
def map_river(dam_code):
    if dam_code in DAM_TO_RIVER:
        return DAM_TO_RIVER[dam_code]
    return "Unknown"

df["river"] = df["dam_code"].apply(map_river)

print("🌊 Added river column based on dam_code mapping")

# ------------------------------------------------------------
# Save output + update original
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Saved → {output_path}")
print(f"🔄 Updated columbiadaily_raw.csv in place")
print("✅ Step 3 complete.")