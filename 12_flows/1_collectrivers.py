# 1_collectrivers.py
# ------------------------------------------------------------
# Step 1 (Flows): Collect all unique river names from the
# z6_plotable_hatcherycounts and z7_plotable_columbiacounts
# tables inside pdf_data.sqlite.
#
# Outputs (two identical CSVs):
#   • 100_Data/flows.csv               (used in pipeline)
#   • 100_Data/1_collectrivers.csv     (frozen snapshot)
#
# Columns:
#   river
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🌊 Step 1: Collecting river names from SQLite database...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

db_path       = data_dir / "pdf_data.sqlite"
flows_path    = data_dir / "flows.csv"
snapshot_path = data_dir / "1_collectrivers.csv"

# ------------------------------------------------------------
# Validate database
# ------------------------------------------------------------
if not db_path.exists():
    raise FileNotFoundError(f"❌ Database not found at: {db_path}\nRun 20_export.py first.")

print(f"📂 Using database: {db_path}")

# ------------------------------------------------------------
# Tables to scan
# ------------------------------------------------------------
tables = [
    "z6_plotable_hatcherycounts",
    "z7_plotable_columbiacounts",
]

river_values = []

# ------------------------------------------------------------
# Extract river names
# ------------------------------------------------------------
with sqlite3.connect(db_path) as conn:
    for table in tables:
        try:
            df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)
        except Exception as e:
            print(f"⚠️ Could not read table '{table}': {e}")
            continue

        print(f"📘 Loaded {len(df):,} rows from {table}")

        if "river" not in df.columns:
            print(f"⚠️ Table '{table}' has no 'river' column — skipping.")
            continue

        river_values.extend(df["river"].astype(str).tolist())

# ------------------------------------------------------------
# Clean + dedupe
# ------------------------------------------------------------
rivers = (
    pd.Series(river_values)
    .astype(str)
    .str.strip()
    .replace("", pd.NA)
    .dropna()
    .drop_duplicates()
    .sort_values()
    .reset_index(drop=True)
)

print(f"🌊 Found {len(rivers):,} unique river names.")

# ------------------------------------------------------------
# Create DataFrame to output
# ------------------------------------------------------------
output_df = pd.DataFrame({"river": rivers})

# ------------------------------------------------------------
# Save both CSVs
# ------------------------------------------------------------
output_df.to_csv(flows_path, index=False)
output_df.to_csv(snapshot_path, index=False)

print(f"💾 Saved pipeline river list → {flows_path}")
print(f"📸 Saved snapshot copy → {snapshot_path}")
print("✅ Step complete — flows.csv and 1_collectrivers.csv created.")