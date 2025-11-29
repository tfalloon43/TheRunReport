# 8.1_delete_snake.py
# ------------------------------------------------------------
# Step 8.1: Remove all rows where river == "Snake River"
#
# Input  : 100_Data/csv_unify_fishcounts.csv
# Output : 100_Data/8.1_delete_snake.csv
#          (and updates csv_unify_fishcounts.csv in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧹 Step 8.1: Removing rows where river = 'Snake River' …")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root  = Path(__file__).resolve().parents[1]
data_dir      = project_root / "100_Data"

input_path    = data_dir / "csv_unify_fishcounts.csv"
output_path   = data_dir / "8.1_delete_snake.csv"
recent_path   = data_dir / "csv_unify_fishcounts.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_unify_fishcounts.csv")

# ------------------------------------------------------------
# Filter rows (remove Snake River)
# ------------------------------------------------------------
before = len(df)

df = df[df["river"] != "Snake River"].reset_index(drop=True)

after = len(df)
removed = before - after

print(f"🗑️ Removed {removed:,} rows where river = 'Snake River'")
print(f"📊 Remaining rows: {after:,}")

# ------------------------------------------------------------
# Save output + update original
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Saved cleaned file → {output_path}")
print(f"🔄 Updated csv_unify_fishcounts.csv in place")
print("✅ Step 8.1 complete.")