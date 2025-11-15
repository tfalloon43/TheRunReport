# 8_currentyear/2_fishperday_current.py
# ------------------------------------------------------------
# Step 2 (Current Year): Calculate average fish per day for each record.
# Creates a new column 'fishperday' equal to:
#   fishperday = adult_diff_plot / day_diff_plot
#
# Notes:
#   • Uses the current-year dataset (csv_currentyear.csv)
#   • Replaces day_diff_plot = 0 with 1 to avoid division errors.
#   • Missing or invalid values are treated as 0.
#   • Values rounded to 3 decimal places.
#
# Input  : 100_Data/csv_currentyear.csv
# Output : 100_Data/2_fishperday_current_output.csv + updated csv_currentyear.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 2 (Current Year): Calculating fishperday (adult_diff_plot ÷ day_diff_plot, rounded to 3 decimals)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_currentyear.csv"
output_path  = data_dir / "2_fishperday_current_output.csv"
recent_path  = data_dir / "csv_currentyear.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(
        f"❌ Missing input file: {input_path}\nRun previous step first."
    )

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
required_cols = ["adult_diff_plot", "day_diff_plot"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Compute fishperday safely
# ------------------------------------------------------------
df["adult_diff_plot"] = pd.to_numeric(df["adult_diff_plot"], errors="coerce").fillna(0)
df["day_diff_plot"]   = pd.to_numeric(df["day_diff_plot"], errors="coerce").replace(0, 1)

df["fishperday"] = (df["adult_diff_plot"] / df["day_diff_plot"]).round(3)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
valid_rows = df["fishperday"].notna().sum()
avg_fish   = df["fishperday"].mean()

print(f"✅ fishperday calculation complete → {output_path}")
print(f"🔄 csv_currentyear.csv updated with new fishperday column")
print(f"📊 {valid_rows:,} rows computed | Mean fish/day = {avg_fish:.3f}")
print("🎯 Example formula:  adult_diff_plot ÷ day_diff_plot (rounded to 3 decimals)")