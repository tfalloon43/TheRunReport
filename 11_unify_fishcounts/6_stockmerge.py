# 6_stockmerge.py
# ------------------------------------------------------------
# Step 6: Merge H/W/U stocks into a single combined stock = "ONE".
#
# For every unique group:
#   (MM-DD, identifier, category_type, metric_type, date_obj, river, Species_Plot)
# compute:
#      value_ONE = sum(values where stock ∈ {H, W, U})
#
# Output:
#   • Adds new rows with stock="ONE"
#   • Saves 6_stockmerge.csv
#   • Updates csv_unify_fishcounts.csv in place
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🔀 Step 6: Merging H/W/U stocks into unified stock='ONE' rows…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root  = Path(__file__).resolve().parents[1]
data_dir      = project_root / "100_Data"

input_path    = data_dir / "csv_unify_fishcounts.csv"
output_path   = data_dir / "6_stockmerge.csv"
recent_path   = data_dir / "csv_unify_fishcounts.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_unify_fishcounts.csv")

# Normalize stock to uppercase
df["stock"] = df["stock"].astype(str).str.upper()

# ------------------------------------------------------------
# Grouping keys for merging stocks
# ------------------------------------------------------------
group_keys = [
    "MM-DD",
    "identifier",
    "category_type",
    "metric_type",
    "date_obj",
    "river",
    "Species_Plot",
]

# ------------------------------------------------------------
# SUM H + W + U for each group
# ------------------------------------------------------------
grouped = (
    df[df["stock"].isin(["H", "W", "U"])]
    .groupby(group_keys, dropna=False)["value"]
    .sum()
    .reset_index()
)

# Assign stock="ONE"
grouped["stock"] = "ONE"

print(f"🧮 Created {len(grouped):,} merged stock rows (H+W+U = ONE)")

# ------------------------------------------------------------
# Combine original df + merged rows
# ------------------------------------------------------------
df_final = pd.concat([df, grouped], ignore_index=True)

print(f"📊 Final row count after merge: {len(df_final):,}")

# ------------------------------------------------------------
# Save output
# ------------------------------------------------------------
df_final.to_csv(output_path, index=False)
df_final.to_csv(recent_path, index=False)

print(f"💾 Saved merged output → {output_path}")
print("🔄 Updated csv_unify_fishcounts.csv in place")
print("✅ Step 6 complete.")