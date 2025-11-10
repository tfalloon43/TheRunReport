# 5_adult_diff.py
# ------------------------------------------------------------
# Step 5: Compute adult_diff = change in Adult_Total since
# previous report for the same facility/species/stock/Stock_BO.
#
# adult_diff = current Adult_Total - previous Adult_Total
# BUT when any of these change:
#   1️⃣ facility
#   2️⃣ species
#   3️⃣ Stock
#   4️⃣ Stock_BO
# then adult_diff = current Adult_Total (reset to current value)
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/5_adult_diff_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 5: Calculating adult_diff with group reset...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "5_adult_diff_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Ensure proper datatypes
# ------------------------------------------------------------
if "date_iso" not in df.columns:
    raise ValueError("❌ Missing 'date_iso' column in input file.")
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")

if "Adult_Total" not in df.columns:
    raise ValueError("❌ Missing 'Adult_Total' column in input file.")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Sort by groups and date
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ------------------------------------------------------------
# Compute adult_diff
# ------------------------------------------------------------
# Base diff within same group
df["adult_diff"] = df.groupby(group_cols)["Adult_Total"].diff()

# Identify where group boundaries change
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)

# Combine all boundaries into one flag
df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)

# When a new group starts, set adult_diff = current Adult_Total
df.loc[df["group_changed"], "adult_diff"] = df.loc[df["group_changed"], "Adult_Total"]

# Fill any NaN diffs (first rows) with their current Adult_Total as well
df["adult_diff"] = df["adult_diff"].fillna(df["Adult_Total"])

# Drop temporary columns
df = df.drop(columns=[f"{col}_changed" for col in group_cols] + ["group_changed"])

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
mean_diff = df["adult_diff"].mean().round(2)
print(f"✅ adult_diff calculation complete → {output_path}")
print(f"🔄 csv_plotdata.csv updated with adult_diff column")
print(f"📊 Mean adult_diff: {mean_diff}")