# 3_reorder.py
# ------------------------------------------------------------
# Step 3: Reorder csv_plotdata.csv rows
# Groups and sorts by:
#   1️⃣ facility
#   2️⃣ species
#   3️⃣ Stock
#   4️⃣ Stock_BO
#   5️⃣ pdf_date (ascending chronological order)
#   6️⃣ Adult_Total (descending → highest count first)
# Input  : 100_Data/csv_plotdata.csv
# Output : 100_Data/3_reorder_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 3: Reordering csv_plotdata.csv by facility, species, stock, Stock_BO, pdf_date, and Adult_Total...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "3_reorder_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun previous step first.")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Convert pdf_date to datetime
# ------------------------------------------------------------
if "pdf_date" not in df.columns:
    raise ValueError("❌ Missing required column: 'pdf_date'. Run 2_pdf_date.py first.")

df["pdf_date"] = pd.to_datetime(df["pdf_date"], errors="coerce")

# ------------------------------------------------------------
# Convert Adult_Total to numeric (if it exists)
# ------------------------------------------------------------
if "Adult_Total" in df.columns:
    df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
else:
    print("⚠️  No Adult_Total column found — sorting by adult count will be skipped.")

# ------------------------------------------------------------
# Sort order definition
# ------------------------------------------------------------
sort_columns = ["facility", "species", "Stock", "Stock_BO", "pdf_date", "Adult_Total"]
ascending_order = [True, True, True, True, True, False]  # Adult_Total is descending

existing_sort_columns = [col for col in sort_columns if col in df.columns]
existing_ascending = [ascending_order[sort_columns.index(col)] for col in existing_sort_columns]

# Perform sort
df_sorted = df.sort_values(by=existing_sort_columns, ascending=existing_ascending, na_position="last")

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
print(f"🎯 Rows are now grouped and ordered by facility → species → Stock → Stock_BO → pdf_date → Adult_Total (desc).")