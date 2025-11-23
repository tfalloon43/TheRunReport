# 4_delete_previous.py
# ------------------------------------------------------------
# Step 4 (Previous Year): Filter out unwanted rows for clarity
#
# Logic:
#   - Remove rows where adult_diff_plot == 0
#   - Keep only rows where Stock is 'H', 'W', or 'U'
#   - Keep only data from the *previous* full calendar year
#       (e.g. if current year is 2025 → keep only 2024)
#
# Input  : 100_Data/csv_previousyear.csv
# Output : 100_Data/4_delete_previous_output.csv
#          + updates csv_previousyear.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
from datetime import datetime

print("🧹 Step 4 (Previous Year): Filtering rows (adult_diff_plot ≠ 0, Stock = H/W/U, previous year only)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_previousyear.csv"
output_path  = data_dir / "4_delete_previous_output.csv"
recent_path  = data_dir / "csv_previousyear.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Clean column names and types
# ------------------------------------------------------------
df.columns = [c.strip() for c in df.columns]
df["adult_diff_plot"] = pd.to_numeric(df["adult_diff_plot"], errors="coerce").fillna(0)
df["Stock"] = df["Stock"].astype(str).str.strip().str.upper()
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")

# ------------------------------------------------------------
# Compute date window (previous full calendar year)
# ------------------------------------------------------------
current_year = datetime.now().year
previous_year = current_year - 1

start_date = pd.Timestamp(f"{previous_year}-01-01")
end_date   = pd.Timestamp(f"{previous_year}-12-31")

print(f"📅 Keeping rows between {start_date.date()} and {end_date.date()}")

# ------------------------------------------------------------
# Apply filters
# ------------------------------------------------------------
before_rows = len(df)
df = df[
    (df["adult_diff_plot"] != 0)
    & (df["Stock"].isin(["H", "W", "U"]))
    & (df["date_iso"].between(start_date, end_date))
]
after_rows = len(df)
removed = before_rows - after_rows

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"✅ Filter complete → {output_path}")
print(f"🗑️ Removed {removed:,} rows ({before_rows:,} → {after_rows:,})")
print(f"📊 Remaining rows: {after_rows:,}")
print(f"📆 Date range: {previous_year} (previous full calendar year)")
print("🎯 csv_previousyear.csv updated with filtered data.")