# 17_NOAAupdate.py
# ------------------------------------------------------------
# Step 17: Add ID column + remove timestamp_dt from NOAA_flows
#
# • Reads NOAA_flows.csv (output of Step 16)
# • Adds "id" column (1, 2, 3, ..., N) as FIRST column
# • Deletes "timestamp_dt"
# • Ensures numeric types for stage_ft and flow_cfs
#
# Input  : 100_Data/NOAA_flows.csv
# Output : 100_Data/17_NOAAupdate.csv
#          (and updates NOAA_flows.csv in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🌊 Step 17: Adding ID + removing timestamp_dt from NOAA flows…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "NOAA_flows.csv"
output_path  = data_dir / "17_NOAAupdate.csv"
update_path  = data_dir / "NOAA_flows.csv"   # overwrite original

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing NOAA flows file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from NOAA_flows.csv")

# ------------------------------------------------------------
# Remove timestamp_dt
# ------------------------------------------------------------
if "timestamp_dt" in df.columns:
    df = df.drop(columns=["timestamp_dt"])
    print("🗑️ Removed 'timestamp_dt' column.")
else:
    print("⚠️ Column 'timestamp_dt' not found — skipping removal.")

# ------------------------------------------------------------
# Convert numeric columns to numeric
# ------------------------------------------------------------
numeric_cols = ["stage_ft", "flow_cfs"]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"🔢 Converted '{col}' to numeric.")
    else:
        print(f"⚠️ Numeric column '{col}' not found — skipping.")

# ------------------------------------------------------------
# Add ID column
# ------------------------------------------------------------
df.insert(0, "id", range(1, len(df) + 1))
print("🆔 Added 'id' column as first column.")

print(f"📊 Final columns: {list(df.columns)}")

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)   # snapshot
df.to_csv(update_path, index=False)  # overwrite NOAA_flows.csv

print("💾 Saved snapshot →", output_path)
print("🔄 Updated NOAA_flows.csv with ID + no timestamp_dt")
print("✅ Step 17 complete.")