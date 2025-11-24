# 3_keep_basinfamily.py
# ------------------------------------------------------------
# Step 3: Filter csv_unify_fishcounts.csv to KEEP ONLY:
#       category_type == "basinfamily"
#
# Output:
#   • Updates csv_unify_fishcounts.csv IN PLACE
#   • Creates 3_keep_basinfamily_output.csv as backup
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧹 Step 3: Keeping only rows where category_type = 'basinfamily'...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_unify_fishcounts.csv"
output_clean = data_dir / "3_keep_basinfamily_output.csv"
recent_path  = data_dir / "csv_unify_fishcounts.csv"  # overwrite

# ------------------------------------------------------------
# Load
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_unify_fishcounts.csv")

# ------------------------------------------------------------
# Validate required column
# ------------------------------------------------------------
if "category_type" not in df.columns:
    raise ValueError("❌ Column 'category_type' missing from CSV.")

# ------------------------------------------------------------
# Filter: keep only basinfamily
# ------------------------------------------------------------
before = len(df)
df_filtered = df[df["category_type"].astype(str).str.lower() == "basinfamily"].copy()
after = len(df_filtered)
removed = before - after

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df_filtered.to_csv(output_clean, index=False)
df_filtered.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"🧽 Removed {removed:,} rows ({before:,} → {after:,})")
print(f"💾 Saved filtered copy → {output_clean}")
print(f"🔄 Updated csv_unify_fishcounts.csv in place")
print("✅ Step 3 complete — basinfamily-only dataset is ready.")