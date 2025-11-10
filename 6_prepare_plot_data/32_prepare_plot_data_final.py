# 32_prepare_plot_data_final.py
# ------------------------------------------------------------
# Step 32 (Final Export): Trim dataset for plotting
#
# This script removes all intermediate iteration columns and
# keeps only the final, clean, plot-ready fields.
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/32_prepare_plot_data_final_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧹 Step 32: Preparing final plot-ready dataset...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "32_prepare_plot_data_final_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
before_cols = len(df.columns)
print(f"✅ Loaded {len(df):,} rows and {before_cols} columns from {input_path.name}")

# ------------------------------------------------------------
# Define final column order
# ------------------------------------------------------------
final_columns = [
    "index",
    "pdf_name",
    "facility",
    "basin",
    "species",
    "Family",
    "Stock_BO",
    "Stock",
    "date_iso",
    "Adult_Total",
    "Jack_Total",
    "Total_Eggtake",
    "On_Hand_Adults",
    "On_Hand_Jacks",
    "Lethal_Spawned",
    "Live_Spawned",
    "Released",
    "Live_Shipped",
    "Mortality",
    "Surplus",
    "pdf_date",
    "day_diff_plot",
    "adult_diff_plot",
    "Biological_Year",
    "Biological_Year_Length",
]

# ------------------------------------------------------------
# Keep only these columns (drop all others)
# ------------------------------------------------------------
missing_cols = [c for c in final_columns if c not in df.columns]
if missing_cols:
    print(f"⚠️ Warning: Missing columns {missing_cols} — continuing with available ones.")

keep_cols = [c for c in final_columns if c in df.columns]
df_final = df[keep_cols].copy()
after_cols = len(df_final.columns)

# ------------------------------------------------------------
# Save final dataset
# ------------------------------------------------------------
df_final.to_csv(output_path, index=False)
df_final.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
dropped_cols = before_cols - after_cols
print(f"✅ Final dataset saved → {output_path}")
print(f"🧾 Columns kept: {after_cols} | Columns dropped: {dropped_cols}")
print(f"📊 Final columns: {', '.join(df_final.columns)}")
print(f"🔢 Total rows: {len(df_final):,}")