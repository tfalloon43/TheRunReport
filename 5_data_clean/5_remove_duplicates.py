# 5_remove_duplicates.py
# ------------------------------------------------------------
# Step 5: Collapse duplicate biological count events (CSV)
#
# Rules:
#   • Same facility, species, Stock_BO
#   • Same full count payload
#   • If events occur within 365 days → keep earliest date_iso
#   • If > 365 days apart → keep both
#
# Input  : 100_Data/csv_reduce.csv
# Output : 100_Data/5_remove_duplicates_output.csv
#          + updates csv_reduce.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 5: Collapsing duplicate biological count events (CSV)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_reduce.csv"
output_path = data_dir / "5_remove_duplicates_output.csv"
recent_path = data_dir / "csv_reduce.csv"

# ------------------------------------------------------------
# Load CSV
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
initial_count = len(df)
print(f"📥 Loaded {initial_count:,} rows")

# ------------------------------------------------------------
# Normalize types (critical for equality)
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")

COUNT_COLS = [
    "Adult_Total", "Jack_Total", "Total_Eggtake",
    "On_Hand_Adults", "On_Hand_Jacks",
    "Lethal_Spawned", "Live_Spawned", "Released",
    "Live_Shipped", "Mortality", "Surplus"
]

for c in COUNT_COLS:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

KEY_COLS = ["facility", "species", "Stock_BO"] + COUNT_COLS

# ------------------------------------------------------------
# Deterministic sort (earliest date wins)
# ------------------------------------------------------------
df = df.sort_values(KEY_COLS + ["date_iso"], kind="mergesort").reset_index(drop=True)

# ------------------------------------------------------------
# Collapse events with 365-day rule
# ------------------------------------------------------------
keep_mask = [False] * len(df)

for _, g in df.groupby(KEY_COLS, dropna=False):
    last_kept_date = None
    for idx in g.index:
        curr_date = df.loc[idx, "date_iso"]

        if last_kept_date is None:
            keep_mask[idx] = True
            last_kept_date = curr_date
        else:
            if (curr_date - last_kept_date).days > 365:
                keep_mask[idx] = True
                last_kept_date = curr_date

df_final = df[keep_mask].reset_index(drop=True)

removed = initial_count - len(df_final)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df_final.to_csv(output_path, index=False)
df_final.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"✅ Event collapse complete → {output_path}")
print(f"🧹 Removed {removed:,} duplicate event rows")
print(f"📊 Final row count: {len(df_final):,}")