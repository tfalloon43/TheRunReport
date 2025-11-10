# 25_cleanup6.py (fixed)
# ------------------------------------------------------------
# Step 25: Cleanup (v6) — Remove biologically invalid negative rows.
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🧹 Step 25 (v6): Cleaning up invalid negative-diff rows...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "25_cleanup6_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "date_iso", "adult_diff6", "by_adult6_length"
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Normalize types
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["adult_diff6"] = pd.to_numeric(df["adult_diff6"], errors="coerce").fillna(0)
df["by_adult6_length"] = pd.to_numeric(df["by_adult6_length"], errors="coerce").fillna(0).astype(int)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# keep original index for deletion tracking
df["orig_index"] = df.index

# ------------------------------------------------------------
# Identify rows to delete
# ------------------------------------------------------------
to_delete = set()

def mark_rows_for_deletion(g):
    # Ensure chronological order and drop old multi-index safely
    g = g.sort_values("date_iso").reset_index(drop=True)

    # Rule 1️⃣: last row negative
    if len(g) > 0 and g.loc[len(g) - 1, "adult_diff6"] < 0:
        to_delete.add(g.loc[len(g) - 1, "orig_index"])

    # Rule 2️⃣: negative diff, by_adult6_length=1, next row negative
    for i in range(len(g) - 1):
        curr = g.loc[i]
        nxt = g.loc[i + 1]
        if (
            curr["adult_diff6"] < 0
            and curr["by_adult6_length"] == 1
            and nxt["adult_diff6"] < 0
        ):
            to_delete.add(curr["orig_index"])

df.groupby(group_cols, group_keys=False).apply(mark_rows_for_deletion)

# ------------------------------------------------------------
# Remove marked rows
# ------------------------------------------------------------
before = len(df)
df = df[~df["orig_index"].isin(to_delete)].reset_index(drop=True)
after = len(df)
removed = before - after

# Drop helper column
df = df.drop(columns=["orig_index"])

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Cleanup complete → {output_path}")
print(f"🧽 Removed {removed:,} rows:")
print(f"   - Last-row negatives")
print(f"   - Isolated short-run negatives (length=1 + following negative diff)")
print(f"📊 Final dataset: {after:,} rows remaining.")