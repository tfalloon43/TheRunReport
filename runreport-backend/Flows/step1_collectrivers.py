# 1_collectrivers.py
# ------------------------------------------------------------
# Step 1 (Flows): Collect unique river names and store them in
# runreport-backend/0_db/local.db as the Flows table.
#
# Sources (if present):
#   - EscapementReport_PlotData.river
#   - Columbia_FishCounts.river
#
# Output table:
#   - Flows (columns: river)
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🌊 Step 1 (Flows): Collecting river names into local.db...")

TABLE_FLOWS = "Flows"
SOURCE_TABLES = [
    ("EscapementReport_PlotData", "river"),
    ("Columbia_FishCounts", "river"),
]

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

if not db_path.exists():
    raise FileNotFoundError(f"❌ local.db not found at {db_path}")

# ------------------------------------------------------------
# COLLECT RIVERS
# ------------------------------------------------------------
river_values: list[str] = []

with sqlite3.connect(db_path) as conn:
    for table, col in SOURCE_TABLES:
        try:
            df = pd.read_sql_query(f"SELECT DISTINCT [{col}] AS river FROM [{table}];", conn)
        except Exception as e:
            print(f"⚠️ Could not read {table}.{col}: {e}")
            continue

        river_values.extend(df["river"].astype(str).tolist())

if not river_values:
    raise RuntimeError(
        "❌ No rivers found. Expected a 'river' column in one of: "
        + ", ".join([t for t, _ in SOURCE_TABLES])
    )

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

output_df = pd.DataFrame({"river": rivers})
print(f"🌊 Found {len(output_df):,} unique rivers")

with sqlite3.connect(db_path) as conn:
    output_df.to_sql(TABLE_FLOWS, conn, if_exists="replace", index=False)

print(f"✅ Step 1 complete — wrote {len(output_df):,} rivers to table [{TABLE_FLOWS}].")

