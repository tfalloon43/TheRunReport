# 11_id.py
# ------------------------------------------------------------
# Step 11: Add ID Column + Convert Value Column to Numeric
#
# • Reads csv_unify_fishcounts.csv
# • Adds new "id" column (1, 2, 3, ..., N)
# • Converts "value" column to numeric (so Supabase infers numeric type)
# • Inserts "id" as the FIRST column
#
# Input  : 100_Data/csv_unify_fishcounts.csv
# Output : 100_Data/11_id.csv
#          (and updates csv_unify_fishcounts.csv)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🔢 Step 11: Adding ID column + converting 'value' to numeric…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "csv_unify_fishcounts.csv"
output_path  = data_dir / "11_id.csv"
update_path  = data_dir / "csv_unify_fishcounts.csv"   # overwrite same file

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows")

# ------------------------------------------------------------
# Add ID column
# ------------------------------------------------------------
df.insert(0, "id", range(1, len(df) + 1))

print("🆔 Added 'id' column as the first column.")

# ------------------------------------------------------------
# Convert 'value' column to numeric
# ------------------------------------------------------------
if "value" in df.columns:
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    print("🔢 Converted 'value' column to numeric.")
else:
    print("⚠️ WARNING: 'value' column not found — cannot convert.")

print(f"📊 Columns now: {list(df.columns)}")

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)   # snapshot
df.to_csv(update_path, index=False)  # overwrite original

print("💾 Saved snapshot →", output_path)
print("🔄 Updated csv_unify_fishcounts.csv with ID + numeric value")
print("✅ Step 11 complete.")