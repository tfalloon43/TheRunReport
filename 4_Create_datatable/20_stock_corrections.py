# 20_stock_corrections.py
# ------------------------------------------------------------
# Step 20 of Create Datatable Pipeline
# Builds the 'stock_correction' column using Stock_BO_corrections
# from lookup_maps.py.
# Logic:
#   • Looks up each Stock_BO (case-insensitive) in Stock_BO_corrections.
#   • If found → assign corrected Stock_BO value.
#   • If not found or Stock_BO is blank → leave blank.
# Input  : 100_Data/csv_recent.csv
# Output : 100_Data/20_stock_corrections_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
import sys
import os

print("🏗️ Step 20: Correcting Stock_BO entries using lookup_maps...")

# ------------------------------------------------------------
# Setup paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

# ✅ Ensure lookup_maps.py can be imported
root_path = str(project_root.resolve())
if root_path not in sys.path:
    sys.path.insert(0, root_path)

# Import Stock_BO_corrections
try:
    from lookup_maps import Stock_BO_corrections  # type: ignore
    print("✅ Successfully imported Stock_BO_corrections from lookup_maps.py")
except ModuleNotFoundError as e:
    print("❌ Could not import lookup_maps.py — ensure it’s at project root")
    raise e

# ------------------------------------------------------------
# File paths
# ------------------------------------------------------------
input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "20_stock_corrections_output.csv"
recent_path = data_dir / "csv_recent.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing {input_path} — run previous step first.")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Build stock_correction column
# ------------------------------------------------------------
if "Stock_BO" not in df.columns:
    raise ValueError("❌ Missing 'Stock_BO' column in input file.")

def correct_stock(stock_value):
    """Lookup and correct Stock_BO values (case-insensitive)."""
    if not isinstance(stock_value, str) or not stock_value.strip():
        return ""
    key = stock_value.strip().upper()
    return Stock_BO_corrections.get(key, stock_value.strip())

df["stock_correction"] = df["Stock_BO"].apply(correct_stock)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
corrected = (df["stock_correction"] != df["Stock_BO"]).sum()
total = len(df)
print(f"✅ Stock correction complete → {output_path}")
print(f"🔄 csv_recent.csv updated with stock_correction column")
print(f"📊 {corrected} of {total} rows had Stock_BO values corrected.")
