# 2_reorder.py
# ------------------------------------------------------------
# Step 2: Reorder csv_plotdata.csv rows
# Groups and sorts by:
#   1️⃣ facility
#   2️⃣ species
#   3️⃣ Stock
#   4️⃣ Stock_BO
#   5️⃣ date_iso (ascending chronological order)
# Input  : 100_Data/csv_plotdata.csv
# Output : 100_Data/2_reorder_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 2: Reordering csv_plotdata.csv by facility, species, stock, Stock_BO, and date_iso...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "2_reorder_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun previous step first.")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Sort by the desired columns
# ------------------------------------------------------------
sort_columns = ["facility", "species", "Stock", "Stock_BO", "date_iso"]
existing_sort_columns = [col for col in sort_columns if col in df.columns]

if not existing_sort_columns:
    raise ValueError("❌ None of the expected sort columns were found in the dataset.")

# Convert date_iso to datetime for accurate sorting
if "date_iso" in df.columns:
    df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")

# Perform sort
df_sorted = df.sort_values(by=existing_sort_columns, ascending=[True, True, True, True, True], na_position="last")

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df_sorted.to_csv(output_path, index=False)
df_sorted.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"✅ Sorting complete → {output_path}")
print(f"🔄 csv_plotdata.csv updated with reordered rows")
print(f"📊 Final row count: {len(df_sorted):,}")
print("🎯 Rows are now grouped and ordered by facility → species → Stock → Stock_BO → date_iso.")