# 5_days_previous.py
# ------------------------------------------------------------
# Step 5 (Previous Year): Expand date_iso into Day1 → DayN columns
# based on day_diff_plot
#
# Logic:
#   - For each row, use date_iso as the end date
#   - Count backwards by day_diff_plot days
#   - Each day (MM-DD) becomes a new column (Day1, Day2, ...)
#
# Input  : 100_Data/csv_previousyear.csv
# Output : 100_Data/5_days_previous_output.csv
#          + updated csv_previousyear.csv
# ------------------------------------------------------------

import pandas as pd
from datetime import timedelta
from pathlib import Path

print("🏗️ Step 5 (Previous Year): Expanding date_iso into Day1 → DayN columns...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_previousyear.csv"
output_path  = data_dir / "5_days_previous_output.csv"
recent_path  = data_dir / "csv_previousyear.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
required_cols = ["date_iso", "day_diff_plot"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Normalize and convert
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["day_diff_plot"] = pd.to_numeric(df["day_diff_plot"], errors="coerce").fillna(0).astype(int)

# ------------------------------------------------------------
# Prepare Day1 → DayN columns
# ------------------------------------------------------------
max_days = int(df["day_diff_plot"].max())
print(f"📅 Maximum day_diff_plot = {max_days} days → Creating up to {max_days} Day columns")

day_cols = [f"Day{i+1}" for i in range(max_days)]
for col in day_cols:
    df[col] = ""

# ------------------------------------------------------------
# Fill in Day columns
# ------------------------------------------------------------
for idx, row in df.iterrows():
    end_date = row["date_iso"]
    days_back = row["day_diff_plot"]

    if pd.isna(end_date) or days_back <= 0:
        continue

    year_start = pd.Timestamp(year=end_date.year, month=1, day=1)

    for i in range(days_back):
        day_label = f"Day{i+1}"
        day_candidate = end_date - timedelta(days=i)

        if day_candidate < year_start:
            df.at[idx, day_label] = year_start.strftime("%m-%d")
            break

        df.at[idx, day_label] = day_candidate.strftime("%m-%d")

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"✅ Created {len(day_cols)} Day columns (Day1 → Day{max_days})")
print(f"💾 Saved → {output_path}")
print("🎯 csv_previousyear.csv updated with daily expansion columns.")