# 5_flowpresence.py
# ------------------------------------------------------------
# Step 5 (Flows): Add flow_presence column to the Flows table.
#
# Logic:
#   - If "Site 1" contains ANY non-empty value → flow_presence = "USGS"
#   - Else → blank
#
# Column order:
#   river | flow_presence | river_name | ...
#
# Input table:
#   - Flows
#
# Output table (overwritten):
#   - Flows
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("💧 Step 5 (Flows): Adding flow_presence column to Flows...")

TABLE_FLOWS = "Flows"

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

with sqlite3.connect(db_path) as conn:
    try:
        df = pd.read_sql_query(f"SELECT * FROM [{TABLE_FLOWS}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_FLOWS}] in local.db: {e}")

print(f"📘 Loaded {len(df):,} rows from [{TABLE_FLOWS}]")

if "Site 1" not in df.columns:
    raise ValueError(f"❌ [{TABLE_FLOWS}] does not contain a 'Site 1' column.")


def has_usgs(val) -> str:
    if pd.isna(val):
        return ""
    if str(val).strip() == "":
        return ""
    return "USGS"


df["flow_presence"] = df["Site 1"].apply(has_usgs)

cols = df.columns.tolist()
for required in ("river", "river_name"):
    if required not in cols:
        raise ValueError(f"❌ Required column missing from [{TABLE_FLOWS}]: {required}")

if "flow_presence" in cols:
    cols.remove("flow_presence")

# Rebuild column order: river, flow_presence, river_name, rest...
new_cols = ["river", "flow_presence", "river_name"] + [c for c in cols if c not in {"river", "river_name"}]
df = df[new_cols]

with sqlite3.connect(db_path) as conn:
    df.to_sql(TABLE_FLOWS, conn, if_exists="replace", index=False)

print("✅ Step 5 complete — flow_presence column added to Flows.")

