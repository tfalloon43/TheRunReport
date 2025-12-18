# 11_merge2.py
# ------------------------------------------------------------
# Step 11 (Flows): Correct NOAA merge that APPENDS Site/Gage pairs
# without deleting existing USGS data. flow_presence is NOT touched.
#
# Input tables:
#   - Flows
#   - Flows_NOAAsites
#
# Output tables (overwritten):
#   - Flows
#   - Flows_NOAAsites (marks added="Y" for matched rivers)
# ------------------------------------------------------------

import re
import sqlite3
from pathlib import Path

import pandas as pd

print("🌧️ Step 11 (Flows): APPENDING NOAA gauge info into Flows (non-destructive)…")

TABLE_FLOWS = "Flows"
TABLE_NOAA_SITES = "Flows_NOAAsites"

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

with sqlite3.connect(db_path) as conn:
    try:
        flows = pd.read_sql_query(f"SELECT * FROM [{TABLE_FLOWS}];", conn).astype("string").fillna("")
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_FLOWS}] in local.db: {e}")

    try:
        noaa = pd.read_sql_query(f"SELECT * FROM [{TABLE_NOAA_SITES}];", conn).astype("string").fillna("")
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_NOAA_SITES}] in local.db: {e}")

print(f"📘 Loaded {len(flows)} rivers from [{TABLE_FLOWS}]")
print(f"📘 Loaded {len(noaa)} NOAA entries from [{TABLE_NOAA_SITES}]")

if "river" not in flows.columns:
    raise ValueError(f"❌ [{TABLE_FLOWS}] missing required column: river")
if "river" not in noaa.columns:
    raise ValueError(f"❌ [{TABLE_NOAA_SITES}] missing required column: river")

if "added" not in noaa.columns:
    noaa["added"] = ""

pattern = re.compile(r"Site (\d+)$")


def next_site_index(row: pd.Series) -> int:
    nums: list[int] = []
    for col in flows.columns:
        m = pattern.match(str(col))
        if m:
            n = int(m.group(1))
            val = str(row.get(col, "")).strip()
            if val:
                nums.append(n)
    return max(nums) + 1 if nums else 1


total_added = 0
rivers_updated = 0

for idx, row in flows.iterrows():
    river = str(row["river"]).strip().lower()
    if not river:
        continue

    match = noaa[noaa["river"].astype(str).str.lower() == river]
    if match.empty:
        continue

    rivers_updated += 1
    start_index = next_site_index(row)
    local_i = 0

    for _, r in match.iterrows():
        k = 1
        while f"Site {k}" in r.index:
            site_val = str(r.get(f"Site {k}", "")).strip()
            gage_val = str(r.get(f"Gage #{k}", "")).strip()

            if site_val and gage_val:
                flows.at[idx, f"Site {start_index + local_i}"] = site_val
                flows.at[idx, f"Gage #{start_index + local_i}"] = gage_val
                local_i += 1
                total_added += 1

            k += 1
            if k > 50:
                break

    noaa.loc[match.index, "added"] = "Y"

print(f"🌧️ NOAA sites appended: {total_added}")
print(f"📍 Rivers updated with NOAA data: {rivers_updated}")

with sqlite3.connect(db_path) as conn:
    flows.to_sql(TABLE_FLOWS, conn, if_exists="replace", index=False)
    noaa.to_sql(TABLE_NOAA_SITES, conn, if_exists="replace", index=False)

print(f"🔄 Updated [{TABLE_FLOWS}] in place")
print(f"🔄 Updated [{TABLE_NOAA_SITES}] in place")
print("🎉 Step 11 complete — NOAA appended, flow_presence untouched.")

