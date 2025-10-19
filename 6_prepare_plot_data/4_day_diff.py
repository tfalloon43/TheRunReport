# 3_day_diff.py
# ------------------------------------------------------------
# Step 3: Calculate day_diff — number of days since previous report
# for the same facility/species/Stock_BO.
# When a new group starts, day_diff = 7 (default weekly cycle).
# Input  : 100_Data/csv_plotdata.csv
# Output : 100_Data/3_day_diff_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 3: Calculating day_diff between updates...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "3_day_diff_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Convert date_iso to datetime
# ------------------------------------------------------------
if "date_iso" not in df.columns:
    raise ValueError("❌ Missing 'date_iso' column in input file.")
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")

# ------------------------------------------------------------
# Compute day_diff (difference in days since previous row)
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock_BO"]

df["day_diff"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)  # Default to 7 days on group reset
    .astype(int)
)

# ------------------------------------------------------------
# Save Outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
mean_diff = df["day_diff"].mean().round(2)
max_diff = df["day_diff"].max()

print(f"✅ day_diff calculation complete → {output_path}")
print(f"🔄 csv_plotdata.csv updated with day_diff column")
print(f"📊 Avg days between updates: {mean_diff}")
print(f"📈 Max days between updates: {max_diff}")