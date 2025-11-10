# 7_by_adult.py
# ------------------------------------------------------------
# Step 7: Assign by_adult (biological year based on adult behavior)
#
# Rules:
# - Reset by_adult to 1 when any of these change:
#       facility, species, Stock, Stock_BO
# - Increment by_adult (+1) when:
#       1) adult_diff < 0  → reset in counts
#       2) day_diff  > 60 → long time gap (new run)
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/7_by_adult_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 7: Assigning by_adult (biological year based on adult trends)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "7_by_adult_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Ensure required columns exist
# ------------------------------------------------------------
required_cols = ["facility", "species", "Stock", "Stock_BO", "day_diff", "adult_diff"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Type conversion
# ------------------------------------------------------------
df["day_diff"] = pd.to_numeric(df["day_diff"], errors="coerce").fillna(0)
df["adult_diff"] = pd.to_numeric(df["adult_diff"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Sort for stability
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]
if "date_iso" in df.columns:
    df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
    df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)
else:
    df = df.sort_values(group_cols).reset_index(drop=True)

# ------------------------------------------------------------
# Assign by_adult
# ------------------------------------------------------------
by_adult = []
current_year = 1
prev_keys = None

for i, row in df.iterrows():
    keys = tuple(row[col] for col in group_cols)
    day_diff = row["day_diff"]
    adult_diff = row["adult_diff"]

    # New group
    if keys != prev_keys:
        current_year = 1
        prev_keys = keys

    # Biological reset
    elif pd.notna(adult_diff) and pd.notna(day_diff):
        if adult_diff < 0 or day_diff > 60:
            current_year += 1

    by_adult.append(current_year)

df["by_adult"] = by_adult

# ------------------------------------------------------------
# Save Output
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
max_by = int(df["by_adult"].max())
unique_groups = len(df[group_cols].drop_duplicates())
num_resets = (df["by_adult"].diff() > 0).sum()

print(f"✅ by_adult assignment complete → {output_path}")
print(f"📊 Max by_adult ID: {max_by}")
print(f"📈 Across {unique_groups:,} unique facility/species/stock groups.")
print(f"🔄 Detected {num_resets:,} by_adult transitions overall.")