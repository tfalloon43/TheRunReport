# 2_date_blank.py
# ------------------------------------------------------------
# Step 2 of Data Clean Pipeline
# Removes rows where the 'date' (or 'date_iso') column is blank.
# Input  : 100_Data/csv_reduce.csv
# Output : 100_Data/2_date_blank_output.csv + updated csv_reduce.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 2: Removing rows with blank date fields...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_reduce.csv"
output_path = data_dir / "2_date_blank_output.csv"
recent_path = data_dir / "csv_reduce.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Remove rows with blank date/date_iso
# ------------------------------------------------------------
# Prefer 'date_iso' if it exists (cleaned version of date)
date_column = "date_iso" if "date_iso" in df.columns else "date"

if date_column not in df.columns:
    raise ValueError(f"❌ Missing date column ('date' or 'date_iso') in CSV.")

before_count = len(df)

# Drop rows where date/date_iso is blank, NaN, or just whitespace
df = df[df[date_column].astype(str).str.strip().ne("") & df[date_column].notna()]
df = df.reset_index(drop=True)

after_count = len(df)
removed = before_count - after_count

print(f"🧹 Removed {removed:,} rows with blank {date_column} values.")
print(f"✅ {after_count:,} rows remain.")

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Output saved → {output_path}")
print(f"🔄 csv_reduce.csv updated (non-destructive chain)")