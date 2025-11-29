# 8_delete_states.py
# ------------------------------------------------------------
# Step 8: Keep only rows where State == "WA"
#
# Input:
#   100_Data/csv_NOAA_completelist.csv
#
# Output:
#   100_Data/8_delete_states.csv     (snapshot)
#   csv_NOAA_completelist.csv        (updated in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧹 Step 8: Filtering NOAA list to only include State = 'WA' ...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root  = Path(__file__).resolve().parents[1]
data_dir      = project_root / "100_Data"

input_path    = data_dir / "csv_NOAA_completelist.csv"
output_path   = data_dir / "8_delete_states.csv"
recent_path   = input_path   # overwrite

# ------------------------------------------------------------
# Load
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_NOAA_completelist.csv")

# ------------------------------------------------------------
# Validate column
# ------------------------------------------------------------
if "State" not in df.columns:
    raise ValueError("❌ Column 'State' not found in csv_NOA A_completelist.csv")

# ------------------------------------------------------------
# Filter for WA only
# ------------------------------------------------------------
before = len(df)
df_filtered = df[df["State"].astype(str).str.upper() == "WA"].reset_index(drop=True)
after = len(df_filtered)
removed = before - after

print(f"🗑️ Removed {removed:,} rows (kept only State = 'WA')")
print(f"📊 Remaining rows: {after:,}")

# ------------------------------------------------------------
# Save output + update
# ------------------------------------------------------------
df_filtered.to_csv(output_path, index=False)
df_filtered.to_csv(recent_path, index=False)

print(f"💾 Saved filtered snapshot → {output_path}")
print(f"🔄 Updated csv_NOAA_completelist.csv in place")
print("✅ Step 8 complete.")