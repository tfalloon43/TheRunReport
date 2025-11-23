# 6_tablegen_previous.py
# ------------------------------------------------------------
# Step 6 (Previous Year): Generate empty 366-day (includes Dec 31)
# tables for:
#   • hatchspecies_[h/w/u]_previous.csv
#   • hatchfamily_[h/w/u]_previous.csv
#   • basinfamily_[h/w/u]_previous.csv
#   • basinspecies_[h/w/u]_previous.csv
#
# Each table:
#   - Rows: Every day of the previous calendar year
#   - Columns: Unique values from the given category (H/W/U)
#
# Input  : 100_Data/csv_previousyear.csv
# Output : 12 stock-specific *_previous.csv tables in 100_Data/
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

print("🏗️ Step 6 (Previous Year): Generating 366-day template tables (Stock = H/W/U)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
input_path   = data_dir / "csv_previousyear.csv"

# Output paths
table_categories = ["hatchspecies", "hatchfamily", "basinfamily", "basinspecies"]
stocks = ["H", "W", "U"]

outputs = {
    f"{category}_{stock.lower()}": data_dir / f"{category}_{stock.lower()}_previous.csv"
    for category in table_categories
    for stock in stocks
}

# ------------------------------------------------------------
# Load input (force string type to avoid dtype issues)
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(
        f"❌ Missing input file: {input_path}\nRun previous steps first."
    )

df = pd.read_csv(input_path, dtype=str)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Normalize columns
# ------------------------------------------------------------
df.columns = [c.strip() for c in df.columns]
df["Stock"] = df["Stock"].astype(str).str.strip().str.upper()

# ------------------------------------------------------------
# Build 366-day MM-DD set for previous year
# ------------------------------------------------------------
# Always base on a leap-year template (2024 calendar)
dates = [
    (datetime(2024, 1, 1) + timedelta(days=i)).strftime("%m-%d")
    for i in range(366)
]

base_df = pd.DataFrame({"MM-DD": dates})
print(f"📅 Created base date frame with {len(base_df)} days (MM-DD, includes 12-31)")

# ------------------------------------------------------------
# Helper function to generate tables
# ------------------------------------------------------------
def make_table(column_name, stock_value, output_path):
    if column_name not in df.columns:
        print(f"⚠️ Skipping {column_name}: column missing.")
        return

    subset = df[df["Stock"] == stock_value]
    uniques = sorted(subset[column_name].dropna().unique())

    print(f"📊 Found {len(uniques)} unique '{column_name}' values for Stock={stock_value}")

    zero_matrix = pd.DataFrame(
        np.zeros((len(base_df), len(uniques))),
        columns=uniques
    )

    table = pd.concat([base_df, zero_matrix], axis=1)
    table.to_csv(output_path, index=False)

    print(f"✅ Saved → {output_path.name} ({len(table)} rows, {len(table.columns)} columns)")

# ------------------------------------------------------------
# Create all 12 stock/category tables
# ------------------------------------------------------------
for category in table_categories:
    for stock in stocks:
        make_table(category, stock, outputs[f"{category}_{stock.lower()}"])

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
print(f"🎯 All {len(outputs)} *_previous template tables generated successfully in 100_Data/")
