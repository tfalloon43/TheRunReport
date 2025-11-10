# 29_cleanup9.py
# ------------------------------------------------------------
# Step 29 (v9): Cleanup — remove weak short-run clusters (x_count8 = 1, 2, or 3)
#
# Rules:
#   - Work across all rows.
#   - Delete rows where x_count8 ∈ [1, 2, 3]
#   - EXCEPTION: keep rows where date_iso is in the current year.
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/29_cleanup9_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
from datetime import datetime

print("🧹 Step 29 (v9): Cleaning up weak short-run clusters (x_count8 = 1–3)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "29_cleanup9_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Normalize types
# ------------------------------------------------------------
if "date_iso" not in df.columns or "x_count8" not in df.columns:
    raise ValueError("❌ Missing required columns: 'date_iso' or 'x_count8'")

df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["x_count8"] = pd.to_numeric(df["x_count8"], errors="coerce").fillna(0).astype(int)

# ------------------------------------------------------------
# Define filter logic
# ------------------------------------------------------------
current_year = datetime.now().year
before_len = len(df)

# Keep rows where:
#   - x_count8 NOT in [1,2,3]
#   OR date_iso year == current_year
mask_keep = (~df["x_count8"].isin([1, 2, 3])) | (df["date_iso"].dt.year == current_year)

removed = before_len - mask_keep.sum()
df = df[mask_keep].reset_index(drop=True)

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Cleanup complete → {output_path}")
print(f"🧽 Removed {removed:,} rows with x_count8 ∈ [1, 2, 3] (excluding {current_year} data).")
print(f"📊 Final dataset: {len(df):,} rows remaining.")
