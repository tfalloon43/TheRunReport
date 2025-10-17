# 4_column_reorg.py
# ------------------------------------------------------------
# Step 4 of Clean Tables Pipeline
# Reorganizes columns into a standardized order and removes
# unnecessary intermediate processing fields.
# Input  : 100_Data/csv_reduce.csv
# Output : 100_Data/4_column_reorg_output.csv + updated csv_reduce.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 4: Reorganizing columns and removing temporary fields...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_reduce.csv"
output_path = data_dir / "4_column_reorg_output.csv"
recent_path = data_dir / "csv_reduce.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Columns to Keep (in desired order)
# ------------------------------------------------------------
ordered_columns = [
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
]

# ------------------------------------------------------------
# Columns to Remove
# ------------------------------------------------------------
drop_columns = [
    "id",
    "page_num",
    "text_line",
    "date",
    "stock_presence",
    "stock_presence_lower",
    "Hatchery_Name",
    "TL2",
    "TL3",
    "count_data",
    "TL4",
    "TL5",
    "TL6",
]

# ------------------------------------------------------------
# Drop extra columns (ignore if missing)
# ------------------------------------------------------------
existing_drop_cols = [c for c in drop_columns if c in df.columns]
df = df.drop(columns=existing_drop_cols, errors="ignore")

# ------------------------------------------------------------
# Ensure only desired columns are included (in proper order)
# ------------------------------------------------------------
existing_ordered = [c for c in ordered_columns if c in df.columns]
df = df[existing_ordered]

# ------------------------------------------------------------
# Save Outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"✅ Column reorganization complete → {output_path}")
print(f"🔄 csv_reduce.csv updated with clean column order")
print(f"📊 Final dataset contains {len(df.columns)} columns and {len(df):,} rows.")