# 8_tablegen.py
# ------------------------------------------------------------
# Step 8: Generate empty 366-day (includes Dec 31) tables for:
#   1. hatchspecies_h.csv
#   2. hatchspecies_w.csv
#   3. hatchfamily_h.csv
#   4. hatchfamily_w.csv
#   5. basinfamily_h.csv
#   6. basinfamily_w.csv
#   7. basinspecies_h.csv
#   8. basinspecies_w.csv
#
# Each table:
#   - Rows: Every day of the year (MM-DD) including 12-31
#   - Columns: Unique values from the given category,
#              filtered by Stock = 'H' or 'W'
#
# Input  : 100_Data/csv_10av.csv
# Output : 8 stock-specific tables in 100_Data/
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

print("🏗️ Step 8: Generating 366-day template tables (H vs W splits, including basinspecies)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
input_path   = data_dir / "csv_10av.csv"

# Output paths
outputs = {
    "hatchspecies_h": data_dir / "hatchspecies_h.csv",
    "hatchspecies_w": data_dir / "hatchspecies_w.csv",
    "hatchfamily_h":  data_dir / "hatchfamily_h.csv",
    "hatchfamily_w":  data_dir / "hatchfamily_w.csv",
    "basinfamily_h":  data_dir / "basinfamily_h.csv",
    "basinfamily_w":  data_dir / "basinfamily_w.csv",
    "basinspecies_h": data_dir / "basinspecies_h.csv",
    "basinspecies_w": data_dir / "basinspecies_w.csv",
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
dates = [(datetime(2024, 1, 1) + timedelta(days=i)).strftime("%m-%d") for i in range(366)]
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
# Create all eight tables
# ------------------------------------------------------------
make_table("hatchspecies", "H", outputs["hatchspecies_h"])
make_table("hatchspecies", "W", outputs["hatchspecies_w"])
make_table("hatchfamily",  "H", outputs["hatchfamily_h"])
make_table("hatchfamily",  "W", outputs["hatchfamily_w"])
make_table("basinfamily",  "H", outputs["basinfamily_h"])
make_table("basinfamily",  "W", outputs["basinfamily_w"])
make_table("basinspecies", "H", outputs["basinspecies_h"])
make_table("basinspecies", "W", outputs["basinspecies_w"])

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
print("🎯 All 8 stock-specific template tables generated successfully in 100_Data/")