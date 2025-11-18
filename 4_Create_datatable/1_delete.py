# 1_delete.py
# ------------------------------------------------------------
# Step 1: Remove any rows in csv_recent.csv where text_line
# contains the phrase "Final in-season estimate" (case-insensitive)
#
# Input  : 100_Data/csv_recent.csv
# Output : 100_Data/1_delete.csv  +  updates csv_recent.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧹 Step 1: Removing lines containing 'Final in-season estimate'...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path    = data_dir / "csv_recent.csv"
output_clean  = data_dir / "1_delete.csv"
output_recent = data_dir / "csv_recent.csv"   # overwrite

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_recent.csv")

# ------------------------------------------------------------
# Ensure required column exists
# ------------------------------------------------------------
if "text_line" not in df.columns:
    raise ValueError("❌ Column 'text_line' is missing in csv_recent.csv")

# ------------------------------------------------------------
# Remove rows containing phrase
# ------------------------------------------------------------
phrase = "final in-season estimate"

before = len(df)
mask = ~df["text_line"].astype(str).str.lower().str.contains(phrase)
df_cleaned = df[mask]

after = len(df_cleaned)
removed = before - after

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df_cleaned.to_csv(output_clean, index=False)
df_cleaned.to_csv(output_recent, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"🗑️ Removed {removed:,} rows containing phrase: '{phrase}'")
print(f"📊 Remaining rows: {after:,}")
print(f"💾 Saved cleaned copy → {output_clean}")
print(f"🔄 Updated csv_recent.csv with cleaned data")
print("✅ Step 1 complete.")
