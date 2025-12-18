# 6_NOAAsites.py
# ------------------------------------------------------------
# Step 6 (Flows): Identify rivers that do NOT yet have USGS flow
# coverage (flow_presence is blank) and prepare them for NOAA.
#
# Input table:
#   - Flows
#
# Output table:
#   - Flows_NOAAsites
#
# Columns:
#   river
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🌧️ Step 6 (Flows): Finding rivers without USGS coverage (for NOAA gauges)…")

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
        flows = pd.read_sql_query(f"SELECT * FROM [{TABLE_FLOWS}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_FLOWS}] in local.db: {e}")

print(f"📘 Loaded {len(flows):,} rows from [{TABLE_FLOWS}]")

if "river" not in flows.columns:
    raise ValueError(f"❌ [{TABLE_FLOWS}] is missing a 'river' column.")
if "flow_presence" not in flows.columns:
    raise ValueError(f"❌ [{TABLE_FLOWS}] is missing a 'flow_presence' column.")

fp = flows["flow_presence"]
missing_mask = fp.isna() | (fp.astype(str).str.strip() == "") | (fp.astype(str).str.lower().isin(["nan", "none"]))

missing_rivers_raw = flows.loc[missing_mask, "river"].astype(str).str.strip().tolist()
print(f"🔍 Raw missing-flow rivers (before manual exclusions): {missing_rivers_raw}")

MANUAL_SKIP = {
    "Cottonwood Creek Pond",
    "Orcas Island",
    "Hood Canal",
    "Kalama River",
}

missing_rivers = [r for r in missing_rivers_raw if r and r not in MANUAL_SKIP]
print(f"⏭️ After skipping manual exclusions: {missing_rivers}")

missing_df = pd.DataFrame({"river": sorted(set(missing_rivers))})

with sqlite3.connect(db_path) as conn:
    missing_df.to_sql(TABLE_NOAA_SITES, conn, if_exists="replace", index=False)

print(f"💾 Saved list of rivers needing NOAA sources → table [{TABLE_NOAA_SITES}]")
print("✅ Step 6 complete — missing-flow rivers identified.")

