# 10_reorg.py
# ------------------------------------------------------------
# Step 10: Reorganize unified fishcounts table
#
# Actions:
#   • Drop: category_type, stock, date_obj
#   • Reorder columns to:
#         MM-DD, river, Species_Plot, metric_type, value
#   • Sort rows by:
#         river → Species_Plot → metric_type → MM-DD
#
# Input  : 100_Data/csv_unify_fishcounts.csv
# Output : 100_Data/10_reorg.csv
#          (and updates csv_unify_fishcounts.csv)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧩 Step 10: Reorganizing unified fishcounts table...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_unify_fishcounts.csv"
output_path  = data_dir / "10_reorg.csv"
recent_path  = data_dir / "csv_unify_fishcounts.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows")

# ------------------------------------------------------------
# Drop unwanted columns
# ------------------------------------------------------------
cols_to_drop = ["category_type", "stock", "date_obj"]

for c in cols_to_drop:
    if c in df.columns:
        df = df.drop(columns=c)
        print(f"🗑️ Dropped column: {c}")
    else:
        print(f"⚠️ Column {c} not found — skipping")

# ------------------------------------------------------------
# Validate remaining required columns
# ------------------------------------------------------------
required_cols = ["MM-DD", "river", "Species_Plot", "metric_type", "value"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns after cleanup: {missing}")

# ------------------------------------------------------------
# Reorder columns
# ------------------------------------------------------------
df = df[required_cols]

print("📑 Columns reordered → MM-DD, river, Species_Plot, metric_type, value")

# ------------------------------------------------------------
# Sorting logic:
#   river → Species_Plot → metric_type → MM-DD
# ------------------------------------------------------------
df["MM-DD"] = df["MM-DD"].astype(str)

df = df.sort_values(
    by=["river", "Species_Plot", "metric_type", "MM-DD"],
    ascending=[True, True, True, True]
).reset_index(drop=True)

print("📊 Rows sorted by (river → Species_Plot → metric_type → MM-DD)")

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Saved reorganized file → {output_path}")
print("🔄 Updated csv_unify_fishcounts.csv")
print("✅ Step 10 complete.")