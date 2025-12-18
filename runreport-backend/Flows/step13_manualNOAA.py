# 13_manualNOAA.py
# ------------------------------------------------------------
# Step 13 (Flows): Manually add NOAA (or custom) gauge info into
# the Flows table.
#
# Behavior:
#   - Matches river (case-insensitive)
#   - Adds Site n / Gage #n at the next available slot
#   - Optionally overwrites flow_presence with user-specified value
#
# Input/Output table:
#   - Flows
# ------------------------------------------------------------

import re
import sqlite3
from pathlib import Path

import pandas as pd

print("📝 Step 13 (Flows): Manually inserting NOAA/custom gauge data into Flows…")

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

if "river" not in df.columns:
    raise ValueError(f"❌ [{TABLE_FLOWS}] missing required column: river")

# ------------------------------------------------------------
# MANUAL ENTRIES — USER EDITS HERE
# ------------------------------------------------------------
manual_entries = {
    "Elochoman River": {
        "flow_presence": "NOAA",
        "site_name": "Elochoman River above Cathlamet",
        "gage": "ECHW1",
    },
}

manual_entries = {k: v for k, v in manual_entries.items() if k.strip() != ""}

pattern = re.compile(r"Site (\d+)$")


def next_site_index(row: pd.Series) -> int:
    indices: list[int] = []
    for col in df.columns:
        m = pattern.match(str(col))
        if m:
            n = int(m.group(1))
            if str(row.get(col, "")).strip():
                indices.append(n)
    return max(indices) + 1 if indices else 1


applied = 0
rivers_found = 0

for river_name, info in manual_entries.items():
    desired_fp = str(info.get("flow_presence", "")).strip()
    site_name = str(info.get("site_name", "")).strip()
    gage_code = str(info.get("gage", "")).strip()

    match = df[df["river"].astype(str).str.lower() == river_name.lower()]
    if match.empty:
        print(f"⚠️ River not found in Flows → {river_name}")
        continue

    rivers_found += 1
    for idx in match.index:
        i = next_site_index(df.loc[idx])
        df.at[idx, f"Site {i}"] = site_name
        df.at[idx, f"Gage #{i}"] = gage_code
        applied += 1

        if desired_fp:
            df.at[idx, "flow_presence"] = desired_fp

print(f"🌊 Manual river updates applied: {rivers_found}")
print(f"➕ Total new Site/Gage entries inserted: {applied}")

with sqlite3.connect(db_path) as conn:
    df.to_sql(TABLE_FLOWS, conn, if_exists="replace", index=False)

print("✅ Step 13 complete — Flows updated.")

