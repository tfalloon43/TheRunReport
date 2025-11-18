# 8_currentyear/6_tablegen_current.py
# ------------------------------------------------------------
# Step 6 (Current Year): Generate empty 366-day (includes Dec 31)
# tables for:
#   • hatchspecies_[h/w/u]_current.csv
#   • hatchfamily_[h/w/u]_current.csv
#   • basinfamily_[h/w/u]_current.csv
#   • basinspecies_[h/w/u]_current.csv
#
# Each table:
#   - Rows: Every day of the year (MM-DD) including 12-31
#   - Columns: Unique values from the given category,
#              filtered by Stock = 'H', 'W', or 'U'
#
# Input  : 100_Data/csv_currentyear.csv
# Output : 12 stock-specific *_current.csv tables in 100_Data/
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

print("🏗️ Step 6 (Current Year): Generating 366-day template tables (Stock = H/W/U)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
input_path   = data_dir / "csv_currentyear.csv"

# Output paths
table_categories = ["hatchspecies", "hatchfamily", "basinfamily", "basinspecies"]
stocks = ["H", "W", "U"]
outputs = {
    f"{category}_{stock.lower()}": data_dir / f"{category}_{stock.lower()}_current.csv"
    for category in table_categories
    for stock in stocks
}

# ------------------------------------------------------------
# Load input (force string type to avoid DtypeWarning)
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun previous step first.")
df = pd.read_csv(input_path, dtype=str)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Normalize columns
# ------------------------------------------------------------
df.columns = [c.strip() for c in df.columns]
df["Stock"] = df["Stock"].astype(str).str.strip().str.upper()

# ------------------------------------------------------------
# Generate MM-DD rows for full year (includes Dec 31)
# ------------------------------------------------------------
dates = [(datetime(datetime.now().year, 1, 1) + timedelta(days=i)).strftime("%m-%d") for i in range(366)]
base_df = pd.DataFrame({"MM-DD": dates})
print(f"📅 Created base date frame with {len(base_df)} days (MM-DD format, includes 12-31)")

# ------------------------------------------------------------
# Helper function to create and save table
# ------------------------------------------------------------
def make_table(column_name, stock_value, output_path):
    if column_name not in df.columns:
        print(f"⚠️ Skipping {column_name}: column not found.")
        return

    subset = df[df["Stock"] == stock_value]
    uniques = sorted(subset[column_name].dropna().unique())
    print(f"📊 Found {len(uniques)} unique {column_name} values for Stock = {stock_value}")

    # Pre-build zero matrix for all columns
    zero_matrix = pd.DataFrame(
        np.zeros((len(base_df), len(uniques))),
        columns=uniques
    )
    table = pd.concat([base_df, zero_matrix], axis=1)

    # Save table
    table.to_csv(output_path, index=False)
    print(f"✅ Saved table → {output_path.name} ({len(table)} rows, {len(table.columns)} columns)")

# ------------------------------------------------------------
# Create all stock-specific tables
# ------------------------------------------------------------
for category in table_categories:
    for stock in stocks:
        make_table(category, stock, outputs[f"{category}_{stock.lower()}"])

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
print(f"🎯 All {len(outputs)} stock-specific *_current template tables generated successfully in 100_Data/")
