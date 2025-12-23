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

import sys
import sqlite3
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from publish.supabase_client import SupabaseConfigError, get_supabase_client

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

try:
    client = get_supabase_client()
except SupabaseConfigError as exc:
    raise RuntimeError(f"❌ Supabase not configured: {exc}") from exc

for table, col in SOURCE_TABLES:
    try:
        page_size = 1000
        start = 0
        while True:
            response = (
                client.table(table)
                .select(col)
                .range(start, start + page_size - 1)
                .execute()
            )
            if getattr(response, "error", None):
                raise RuntimeError(response.error)
            rows = response.data or []
            if not rows:
                break
            river_values.extend([str(row.get(col, "")) for row in rows])
            if len(rows) < page_size:
                break
            start += page_size
    except Exception as e:
        print(f"⚠️ Could not read {table}.{col} from Supabase: {e}")
        continue

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
