# 6_surplus_diff.py
# ------------------------------------------------------------
# Step 6: Compute surplus_diff = change in Surplus since
# previous report for the same facility/species/stock/Stock_BO.
#
# surplus_diff = current Surplus - previous Surplus
# BUT when any of these change:
#   1️⃣ facility
#   2️⃣ species
#   3️⃣ Stock
#   4️⃣ Stock_BO
# then surplus_diff = current Surplus (reset to current value)
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/9_surplus_diff_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 6: Calculating surplus_diff with group reset...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "6_surplus_diff_output.csv"
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

if "Surplus" not in df.columns:
    raise ValueError("❌ Missing 'Surplus' column in input file.")
df["Surplus"] = pd.to_numeric(df["Surplus"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Sort by groups and date
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ------------------------------------------------------------
# Compute surplus_diff
# ------------------------------------------------------------
# Base diff within same group
df["surplus_diff"] = df.groupby(group_cols)["Surplus"].diff()

# Identify where group boundaries change
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)

# Combine all boundaries into one flag
df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)

# When a new group starts, set surplus_diff = current Surplus
df.loc[df["group_changed"], "surplus_diff"] = df.loc[df["group_changed"], "Surplus"]

# Fill any NaN diffs (first rows) with their current Surplus as well
df["surplus_diff"] = df["surplus_diff"].fillna(df["Surplus"])

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
mean_diff = df["surplus_diff"].mean().round(2)
print(f"✅ surplus_diff calculation complete → {output_path}")
print(f"🔄 csv_plotdata.csv updated with surplus_diff column")
print(f"📊 Mean surplus_diff: {mean_diff}")