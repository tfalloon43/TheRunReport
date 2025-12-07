# 5_id.py
# ------------------------------------------------------------
# Step 5: Add ID Column + Convert Numeric Columns
#
# • Reads columbiadaily_raw.csv (already reorganized by Step 4)
# • Adds a new "id" column (1, 2, 3, ..., N)
# • Makes "id" the FIRST column
# • Converts numeric columns to numeric so Supabase infers correct types
#
# Input  : 100_Data/columbiadaily_raw.csv
# Output : 100_Data/5_id.csv
#          (and updates columbiadaily_raw.csv in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🔢 Step 5: Adding ID column + converting numeric columns…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "columbiadaily_raw.csv"
output_path  = data_dir / "5_id.csv"
update_path  = data_dir / "columbiadaily_raw.csv"   # overwrite same file

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows")

# ------------------------------------------------------------
# Add ID column
# ------------------------------------------------------------
df.insert(0, "id", range(1, len(df) + 1))
print("🆔 Added 'id' column as the first column.")

# ------------------------------------------------------------
# Convert numeric columns
# ------------------------------------------------------------
numeric_cols = [
    "Daily_Count_Current_Year",
    "Daily_Count_Last_Year",
    "Ten_Year_Average_Daily_Count"
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"🔢 Converted '{col}' to numeric.")
    else:
        print(f"⚠️ WARNING: Column '{col}' not found — skipping.")

print(f"📊 Columns now: {list(df.columns)}")

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(update_path, index=False)

print("💾 Saved →", output_path)
print("🔄 Updated columbiadaily_raw.csv with ID + numeric values")
print("✅ Step 5 complete.")