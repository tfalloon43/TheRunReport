# 50_USGStempupdate.py
# ------------------------------------------------------------
# Step 50: Prepare USGS_flows.csv for Supabase
#
# • Reads USGS_flows.csv (output of Step 15)
# • Adds "id" column (1, 2, 3, ..., N) as FIRST column
# • Converts stage_ft, flow_cfs to numeric
# • Keeps only rows where id % 6 == 0 (1/6 sampling)
#
# Input  : 100_Data/USGS_flows.csv
# Output : 100_Data/50_USGStempupdate.csv
#          (and updates USGS_flows.csv in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🔧 Step 50: Updating USGS_flows.csv for Supabase upload…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "USGS_flows.csv"
output_path  = data_dir / "50_USGStempupdate.csv"
update_path  = data_dir / "USGS_flows.csv"   # overwrite original

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from USGS_flows.csv")

# ------------------------------------------------------------
# Convert numeric columns
# ------------------------------------------------------------
numeric_cols = ["stage_ft", "flow_cfs"]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"🔢 Converted '{col}' to numeric.")
    else:
        print(f"⚠️ WARNING: '{col}' column not found — skipping.")

# ------------------------------------------------------------
# Add ID column
# ------------------------------------------------------------
df.insert(0, "id", range(1, len(df) + 1))
print("🆔 Added 'id' as first column.")

# ------------------------------------------------------------
# Filter rows by id % 6 == 0 (1/6 sampling)
# ------------------------------------------------------------
df_filtered = df[df["id"] % 6 == 0].reset_index(drop=True)
print(f"🔍 Filtered down to {len(df_filtered)} rows (id divisible by 6).")

# ------------------------------------------------------------
# Regenerate ID values (clean sequential IDs after filtering)
# ------------------------------------------------------------
if "id" in df_filtered.columns:
    df_filtered = df_filtered.drop(columns=["id"])

df_filtered.insert(0, "id", range(1, len(df_filtered) + 1))
print("🔄 Re-generated sequential ID values after filtering.")

print(f"📊 Final columns: {list(df_filtered.columns)}")

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df_filtered.to_csv(output_path, index=False)   # snapshot
df_filtered.to_csv(update_path, index=False)  # overwrite final

print("💾 Saved snapshot →", output_path)
print("🔄 Updated USGS_flows.csv with ID + numeric types + 1/6 sampling")
print("✅ Step 50 complete.")