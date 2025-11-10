# 31_iteration10.py
# ------------------------------------------------------------
# Step 31 (v10): Final plotting prep columns
#
# After final biological metrics (_f), this script prepares
# derived plotting columns:
#   ✅ day_diff_plot
#   ✅ adult_diff_plot
#   ✅ Biological_Year
#   ✅ Biological_Year_Length
#
# Logic:
#   1. day_diff_plot = day_diff_f
#      → If by_adult_f == previous by_adult_f + 1 (same group), set to 7
#   2. adult_diff_plot = adult_diff_f
#      → If adult_diff_f < 0, replace with Adult_Total
#   3. Biological_Year = by_adult_f
#   4. Biological_Year_Length = by_adult_f_length
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/31_iteration10_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 31: Building final plotting columns (day_diff_plot → Biological_Year_Length)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "31_iteration10_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Normalize types
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# Sort for consistent grouping
df = df.reset_index(drop=True)
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ============================================================
# STEP 1: day_diff_plot
# ============================================================
print("🔹 Creating day_diff_plot...")
df["day_diff_plot"] = df["day_diff_f"]

# flag transitions where by_adult_f increases by 1 (same biological identity)
df["prev_by"] = df.groupby(group_cols)["by_adult_f"].shift(1)
boundary_mask = df["by_adult_f"] == (df["prev_by"] + 1)
df.loc[boundary_mask, "day_diff_plot"] = 7
boundary_count = boundary_mask.sum()

# cleanup helper column
df = df.drop(columns=["prev_by"])

# ============================================================
# STEP 2: adult_diff_plot
# ============================================================
print("🔹 Creating adult_diff_plot...")
df["adult_diff_plot"] = df["adult_diff_f"]
df.loc[df["adult_diff_f"] < 0, "adult_diff_plot"] = df["Adult_Total"]

# ============================================================
# STEP 3: Biological_Year
# ============================================================
df["Biological_Year"] = df["by_adult_f"]

# ============================================================
# STEP 4: Biological_Year_Length
# ============================================================
df["Biological_Year_Length"] = df["by_adult_f_length"]

# ============================================================
# SAVE OUTPUT
# ============================================================
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Iteration 10 complete → {output_path}")
print(f"📊 Added columns: day_diff_plot, adult_diff_plot, Biological_Year, Biological_Year_Length")
print(f"🔢 Total rows: {len(df):,}")
print(f"🔁 Biological year transitions (day_diff_plot=7): {boundary_count:,}")
