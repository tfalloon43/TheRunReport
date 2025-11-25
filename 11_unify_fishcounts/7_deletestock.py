# 7_deletestock.py
# ------------------------------------------------------------
# Step 7: Keep ONLY rows where stock == "ONE"
#
# Input  : 100_Data/csv_unify_fishcounts.csv
# Output : 100_Data/7_deletestock.csv
#          (and updates csv_unify_fishcounts.csv in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧹 Step 7: Keeping only rows where stock = 'ONE' …")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root  = Path(__file__).resolve().parents[1]
data_dir      = project_root / "100_Data"

input_path    = data_dir / "csv_unify_fishcounts.csv"
output_path   = data_dir / "7_deletestock.csv"
recent_path   = data_dir / "csv_unify_fishcounts.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_unify_fishcounts.csv")

# Normalize stock values
df["stock"] = df["stock"].astype(str).str.upper()

# ------------------------------------------------------------
# Filter rows
# ------------------------------------------------------------
before = len(df)
df = df[df["stock"] == "ONE"].reset_index(drop=True)
after = len(df)

removed = before - after
print(f"🗑️ Removed {removed:,} rows (kept {after:,})")

# ------------------------------------------------------------
# Save output + update original
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Saved cleaned file → {output_path}")
print(f"🔄 Updated csv_unify_fishcounts.csv in place")
print("✅ Step 7 complete — only ONE-stock rows remain.")