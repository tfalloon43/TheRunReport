# 9_Snohomish.py
# ------------------------------------------------------------
# Step 9: Create Snohomish River rows by combining
#         Skykomish + Snoqualmie daily values
#
# Logic:
#   • Keep original Skykomish/Snoqualmie rows
#   • Create NEW rows:
#       river = "Snohomish River"
#       identifier = "Snohomish River - {Species_Plot}"
#       stock = ONE
#       category_type = basinfamily
#
#   • Group rows where:
#         river ∈ {Skykomish River, Snoqualmie River}
#     by:
#         MM-DD, metric_type, Species_Plot
#
#   • Snohomish_value = sum(values from the two rivers)
#       (If only one exists → that value is used)
#
# Output:
#   100_Data/9_Snohomish.csv
#   Updates csv_unify_fishcounts.csv in place
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🌊 Step 9: Creating Snohomish River aggregated rows...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_unify_fishcounts.csv"
output_path  = data_dir / "9_Snohomish.csv"
recent_path  = data_dir / "csv_unify_fishcounts.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_unify_fishcounts.csv")

# ------------------------------------------------------------
# Filter for tributaries Skykomish + Snoqualmie
# ------------------------------------------------------------
trib_rivers = ["Skykomish River", "Snoqualmie River"]

df_sub = df[
    (df["river"].isin(trib_rivers)) &
    (df["category_type"] == "basinfamily") &
    (df["stock"] == "ONE")
].copy()

print(f"📊 Found {len(df_sub):,} Skykomish/Snoqualmie rows to aggregate")

# ------------------------------------------------------------
# Group and SUM values
# ------------------------------------------------------------
group_cols = ["MM-DD", "metric_type", "Species_Plot"]

agg_df = (
    df_sub.groupby(group_cols, as_index=False)["value"]
          .sum(min_count=1)
)

print(f"➕ Aggregated into {len(agg_df):,} Snohomish rows")

# ------------------------------------------------------------
# Build final Snohomish rows
# ------------------------------------------------------------
agg_df["river"]         = "Snohomish River"
agg_df["identifier"]    = agg_df["Species_Plot"].apply(lambda s: f"Snohomish River - {s}")
agg_df["stock"]         = "ONE"
agg_df["category_type"] = "basinfamily"

# Rebuild date_obj (fixed 2024 baseline)
agg_df["date_obj"] = pd.to_datetime(
    "2024-" + agg_df["MM-DD"],
    format="%Y-%m-%d",
    errors="coerce"
)

# Order columns exactly like existing dataset
ordered_cols = [
    "MM-DD", "identifier", "value", "category_type",
    "stock", "metric_type", "date_obj", "river", "Species_Plot"
]
agg_df = agg_df[ordered_cols]

print("🌊 Snohomish rows structured and ready")

# ------------------------------------------------------------
# Append Snohomish to the master dataset
# ------------------------------------------------------------
df_final = pd.concat([df, agg_df], ignore_index=True)

print(f"📈 Final dataset size: {len(df_final):,} rows")

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df_final.to_csv(output_path, index=False)       # full Snohomish output
df_final.to_csv(recent_path, index=False)       # update master file

print(f"💾 Snohomish output saved → {output_path}")
print("🔄 csv_unify_fishcounts.csv updated with Snohomish rows")
print("✅ Step 9 complete — Snohomish aggregation successful.")