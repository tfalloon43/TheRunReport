# 5_remove_duplicates.py
# ------------------------------------------------------------
# Step 5: Remove duplicate rows (ignoring pdf_name and date_iso columns)
# Keeps the first occurrence of each unique combination
# across all other columns.
# Input  : 100_Data/csv_reduce.csv
# Output : 100_Data/5_remove_duplicates_output.csv + updated csv_reduce.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 5: Removing duplicate rows (ignoring pdf_name and date_iso)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_reduce.csv"
output_path = data_dir / "5_remove_duplicates_output.csv"
recent_path = data_dir / "csv_reduce.csv"

# ------------------------------------------------------------
# Load CSV
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun previous step first.")

df = pd.read_csv(input_path)
initial_count = len(df)

# ------------------------------------------------------------
# Remove duplicates (ignore pdf_name and date_iso)
# ------------------------------------------------------------
ignore_cols = {"pdf_name", "date_iso"}
cols_to_check = [c for c in df.columns if c not in ignore_cols]

df_deduped = df.drop_duplicates(subset=cols_to_check, keep="first")
removed = initial_count - len(df_deduped)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df_deduped.to_csv(output_path, index=False)
df_deduped.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"✅ Duplicate removal complete → {output_path}")
print(f"📊 {removed} duplicate rows removed.")
print(f"📈 Final row count: {len(df_deduped):,}")
print(f"🧩 Ignored columns: {', '.join(ignore_cols)}")