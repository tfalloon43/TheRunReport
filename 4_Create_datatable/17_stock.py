# 17_stock.py
# ------------------------------------------------------------
# Extract standalone stock indicator (H/W/U/M/C)
# from the end of the Stock_BO column.
# Only active if Stock_BO has content.
# Input  : 100_Data/csv_recent.csv
# Output : 100_Data/17_stock_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import pandas as pd
import re
from pathlib import Path
import os

# ------------------------------------------------------------
# Setup paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "17_stock_output.csv"
recent_path = data_dir / "csv_recent.csv"

print("🏗️  Step 17: Extracting Stock indicator...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun Step 16 first.")

df = pd.read_csv(input_path)

if "Stock_BO" not in df.columns:
    raise ValueError("❌ 'Stock_BO' column not found in input file.")

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def extract_stock(value):
    """Extract final stock indicator (H/W/U/M/C) at end of string."""
    if not isinstance(value, str) or not value.strip():
        return ""
    match = re.search(r'(?:\b|[-\s])([HWUMC])\s*$', value.strip())
    return match.group(1) if match else ""

# ------------------------------------------------------------
# Apply logic
# ------------------------------------------------------------
df["Stock"] = df["Stock_BO"].apply(extract_stock)

# ------------------------------------------------------------
# Save outputs (in 100_Data)
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
total = len(df)
filled = df["Stock"].astype(str).str.strip().ne("").sum()
print(f"✅ Stock extraction complete → {output_path}")
print(f"🔄 csv_recent.csv updated with Stock column")
print(f"📊 {filled} of {total} rows successfully populated with stock indicators.")