# 3.5_date_AT_same_remove.py
# ------------------------------------------------------------
# Within each biological identity (facility, species, Stock, Stock_BO),
# if multiple rows share the same date_iso and Adult_Total, keep only
# the row with the earliest pdf_date.
# Input  : 100_Data/csv_plotdata.csv
# Output : 100_Data/3.5_date_AT_same_remove_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 3.5: Removing same date_iso + Adult_Total duplicates (keep earliest pdf_date)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "3.5_date_AT_same_remove_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun previous steps first.")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# Ensure required columns exist
required_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total", "pdf_date"]
for col in required_cols:
    if col not in df.columns:
        df[col] = ""

# Parse dates for ordering; keep originals intact
df["_date_iso_dt"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["_pdf_date_dt"] = pd.to_datetime(df["pdf_date"], errors="coerce")
df["_orig_order"] = range(len(df))

key_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total"]

# Sort so earliest pdf_date per key comes first; stable tie-break on original order
df_sorted = df.sort_values(
    by=key_cols + ["_pdf_date_dt", "_orig_order"],
    ascending=[True, True, True, True, True, True, True, True],
    na_position="last",
    kind="mergesort",
)

# Drop duplicates keeping the earliest pdf_date within each key group
df_deduped = df_sorted.drop_duplicates(subset=key_cols, keep="first")

# Remove helper columns
df_deduped = df_deduped.drop(columns=["_date_iso_dt", "_pdf_date_dt", "_orig_order"])

removed = len(df) - len(df_deduped)
print(f"🧹 Removed {removed:,} duplicate rows based on (facility, species, Stock, Stock_BO, date_iso, Adult_Total) keeping earliest pdf_date.")
print(f"📊 Final row count: {len(df_deduped):,}")

# Save outputs
df_deduped.to_csv(output_path, index=False)
df_deduped.to_csv(recent_path, index=False)

print(f"✅ Step 3.5 complete → {output_path}")
print("🔄 csv_plotdata.csv updated after duplicate removal")
