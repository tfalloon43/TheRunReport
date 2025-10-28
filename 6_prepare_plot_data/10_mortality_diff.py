# 10_mortality_diff.py
# ------------------------------------------------------------
# Step 10: Compute mortality_diff = change in Mortality since
# previous report for the same facility/species/stock/Stock_BO.
#
# mortality_diff = current Mortality - previous Mortality
# BUT when any of these change:
#   1️⃣ facility
#   2️⃣ species
#   3️⃣ Stock
#   4️⃣ Stock_BO
# then mortality_diff = current Mortality (reset to current value)
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/10_mortality_diff_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 10: Calculating mortality_diff with group reset...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "10_mortality_diff_output.csv"
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

if "Mortality" not in df.columns:
    raise ValueError("❌ Missing 'Mortality' column in input file.")
df["Mortality"] = pd.to_numeric(df["Mortality"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Sort by groups and date
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ------------------------------------------------------------
# Compute mortality_diff
# ------------------------------------------------------------
# Base diff within same group
df["mortality_diff"] = df.groupby(group_cols)["Mortality"].diff()

# Identify where group boundaries change
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)

# Combine all boundaries into one flag
df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)

# When a new group starts, set mortality_diff = current Mortality
df.loc[df["group_changed"], "mortality_diff"] = df.loc[df["group_changed"], "Mortality"]

# Fill any NaN diffs (first rows) with their current Mortality as well
df["mortality_diff"] = df["mortality_diff"].fillna(df["Mortality"])

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
mean_diff = df["mortality_diff"].mean().round(2)
print(f"✅ mortality_diff calculation complete → {output_path}")
print(f"🔄 csv_plotdata.csv updated with mortality_diff column")
print(f"📊 Mean mortality_diff: {mean_diff}")