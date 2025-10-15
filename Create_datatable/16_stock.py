# 16_stock.py
# ------------------------------------------------------------
# Extract standalone stock indicator (H/W/U/M/C)
# from the end of the Stock_BO column.
# Only active if Stock_BO has content.
# Input  : csv_recent.csv
# Output : 16_stock_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import os
import pandas as pd
import re

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "16_stock_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 16: Extracting Stock indicator...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")

df = pd.read_csv(input_path)

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
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
filled = df["Stock"].astype(str).str.strip().ne("").sum()
print(f"✅ Stock extraction complete → {output_path}")
print(f"🔄 csv_recent.csv updated with Stock column")
print(f"📊 {filled} rows successfully populated with stock indicators.")