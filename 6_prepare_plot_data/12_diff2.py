# 12_diff2.py
# ------------------------------------------------------------
# Step 12: Recalculate day_diff2 and adult_diff2 after row removal
#
# Logic:
# - Recompute both day_diff2 and adult_diff2 for the cleaned dataset.
# - Work within biological identity groups:
#     facility, species, Stock, Stock_BO
#
# day_diff2 = days since previous report in same group
#   - Reset to 7 when a new group starts
#
# adult_diff2 = change in Adult_Total since previous row in same group
#   - Reset to current Adult_Total when a new group starts
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/12_diff2_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 12: Recalculating day_diff2 and adult_diff2 after cleanup...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "12_diff2_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Ensure required columns exist
# ------------------------------------------------------------
required_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Convert datatypes
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Sort by group and date
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ------------------------------------------------------------
# Compute day_diff2
# ------------------------------------------------------------
df["day_diff2"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)  # Default for first record in group
    .astype(int)
)

# ------------------------------------------------------------
# Compute adult_diff2
# ------------------------------------------------------------
df["adult_diff2"] = df.groupby(group_cols)["Adult_Total"].diff()

# Identify where groups reset
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)
df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)

# Reset adult_diff2 to current Adult_Total when a new group starts
df.loc[df["group_changed"], "adult_diff2"] = df.loc[df["group_changed"], "Adult_Total"]

# Fill any remaining NaNs (e.g., first rows)
df["adult_diff2"] = df["adult_diff2"].fillna(df["Adult_Total"])

# Drop temp cols
df = df.drop(columns=[f"{col}_changed" for col in group_cols] + ["group_changed"])

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
mean_day = df["day_diff2"].mean().round(2)
mean_adult = df["adult_diff2"].mean().round(2)
print(f"✅ day_diff2 & adult_diff2 recalculation complete → {output_path}")
print(f"📊 Avg day_diff2: {mean_day}")
print(f"📈 Mean adult_diff2: {mean_adult}")