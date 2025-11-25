# 4_reorg.py
# ------------------------------------------------------------
# Step 4: Reorganize Columbia Daily Data
#
# • Removes unused columns:
#       Dam, dam_code, Species, species_code
# • Reorders columns to:
#       Dates, river, dam_name, Species_Plot,
#       Daily_Count_Current_Year, Daily_Count_Last_Year,
#       Ten_Year_Average_Daily_Count
#
# Input  : 100_Data/columbiadaily_raw.csv
# Output : 100_Data/4_reorg.csv
#          (and updates columbiadaily_raw.csv in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧹 Step 4: Reorganizing Columbia Daily Data…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "columbiadaily_raw.csv"
output_path  = data_dir / "4_reorg.csv"
recent_path  = data_dir / "columbiadaily_raw.csv"   # overwrite input

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows")

# ------------------------------------------------------------
# Columns to remove
# ------------------------------------------------------------
cols_to_drop = ["Dam", "dam_code", "Species", "species_code"]

missing = [c for c in cols_to_drop if c not in df.columns]
if missing:
    print(f"⚠️ Warning: Could not drop missing columns: {missing}")

df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

# ------------------------------------------------------------
# Reorder columns
# ------------------------------------------------------------
desired_order = [
    "Dates",
    "river",
    "dam_name",
    "Species_Plot",
    "Daily_Count_Current_Year",
    "Daily_Count_Last_Year",
    "Ten_Year_Average_Daily_Count",
]

# Validate all required columns exist
missing2 = [c for c in desired_order if c not in df.columns]
if missing2:
    raise ValueError(f"❌ Missing required columns for ordering: {missing2}")

df = df[desired_order]

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print("✅ Reorganization complete")
print(f"💾 Saved → {output_path}")
print(f"🔄 Updated columbiadaily_raw.csv in place")
print(f"📊 Final columns: {list(df.columns)}")