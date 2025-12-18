# 12_flowpresence2.py
# ------------------------------------------------------------
# Step 12 (Flows): Update flow_presence ONLY when:
#   - flow_presence is blank AND
#   - "Site 1" contains ANY value
#
# Then → flow_presence = "NOAA"
#
# Input/Output table:
#   - Flows
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("💧 Step 12 (Flows): Updating flow_presence where appropriate (NOAA only)…")

TABLE_FLOWS = "Flows"

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

with sqlite3.connect(db_path) as conn:
    try:
        df = pd.read_sql_query(f"SELECT * FROM [{TABLE_FLOWS}];", conn).astype("string").fillna("")
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_FLOWS}] in local.db: {e}")

print(f"📘 Loaded {len(df):,} rows from [{TABLE_FLOWS}]")

if "flow_presence" not in df.columns:
    raise ValueError(f"❌ [{TABLE_FLOWS}] missing required column: flow_presence")
if "Site 1" not in df.columns:
    raise ValueError(f"❌ [{TABLE_FLOWS}] missing required column: Site 1")

updates = 0

for idx, row in df.iterrows():
    fp = str(row["flow_presence"]).strip()
    site1 = str(row["Site 1"]).strip()
    if fp == "" and site1 != "":
        df.at[idx, "flow_presence"] = "NOAA"
        updates += 1

print(f"💧 Updated {updates} rows with flow_presence = 'NOAA'")

with sqlite3.connect(db_path) as conn:
    df.to_sql(TABLE_FLOWS, conn, if_exists="replace", index=False)

print("✅ Step 12 complete — Flows updated.")

