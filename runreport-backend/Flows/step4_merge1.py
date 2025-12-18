# 4_merge1.py
# ------------------------------------------------------------
# Step 4 (Flows): Merge USGS site info (site_name + site_number)
# INTO Flows, and mark "added" inside Flows_USGSsites_rivername.
#
# Input tables:
#   - Flows
#   - Flows_USGSsites_rivername
#
# Output tables (overwritten):
#   - Flows
#   - Flows_USGSsites_rivername
#
# Behavior:
#   - For each river in Flows:
#       match river_name (case-insensitive, exact)
#       rewrite clean numbered columns:
#         Site 1 | Gage #1 | Site 2 | Gage #2 | ...
#   - Mark matched rows in the USGS table: added="Y"
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🔗 Step 4 (Flows): Merging USGS site info into Flows (clean & renumbered)...")
print("📝 'added' flags will be stored in Flows_USGSsites_rivername only.")

TABLE_FLOWS = "Flows"
TABLE_USGS_RIVERNAMES = "Flows_USGSsites_rivername"

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

    try:
        sites = pd.read_sql_query(f"SELECT * FROM [{TABLE_USGS_RIVERNAMES}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_USGS_RIVERNAMES}] in local.db: {e}")

print(f"📘 Loaded {len(flows):,} rivers from [{TABLE_FLOWS}]")
print(f"📘 Loaded {len(sites):,} USGS sites from [{TABLE_USGS_RIVERNAMES}]")

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
required_flows = ["river"]
required_sites = ["river_name", "site_name", "site_number"]

for col in required_flows:
    if col not in flows.columns:
        raise ValueError(f"❌ [{TABLE_FLOWS}] missing required column: {col}")

for col in required_sites:
    if col not in sites.columns:
        raise ValueError(f"❌ [{TABLE_USGS_RIVERNAMES}] missing required column: {col}")

# ------------------------------------------------------------
# Prepare sites file: add "added" if not existing
# ------------------------------------------------------------
if "added" not in sites.columns:
    sites["added"] = ""


def clean_site_no(x) -> str:
    try:
        return str(int(float(x))).strip()
    except Exception:
        return ""


sites["site_number_clean"] = sites["site_number"].apply(clean_site_no)

# ------------------------------------------------------------
# Prepare flows: remove all old Site n / Gage #n columns
# ------------------------------------------------------------
site_cols = [c for c in flows.columns if str(c).lower().startswith("site ")]
gage_cols = [c for c in flows.columns if str(c).lower().startswith("gage #")]

flows_clean = flows.drop(columns=site_cols + gage_cols, errors="ignore").copy()
print(f"🧹 Removed {len(site_cols) + len(gage_cols)} old site/gage columns from Flows")

# ------------------------------------------------------------
# Merge logic
# ------------------------------------------------------------
total_added = 0
rivers_with_sites = 0

sites_river_lower = sites["river_name"].astype(str).str.strip().str.lower()

for idx, row in flows_clean.iterrows():
    river = str(row["river"]).strip().lower()
    if not river:
        continue

    matched = sites[sites_river_lower == river]
    if matched.empty:
        continue

    rivers_with_sites += 1
    site_pairs = list(
        zip(
            matched["site_name"].astype(str).tolist(),
            matched["site_number_clean"].astype(str).tolist(),
        )
    )

    for i, (sname, snum) in enumerate(site_pairs, start=1):
        flows_clean.at[idx, f"Site {i}"] = sname
        flows_clean.at[idx, f"Gage #{i}"] = snum
        total_added += 1

    sites.loc[matched.index, "added"] = "Y"

print(f"🧩 Added {total_added:,} site/gage entries")
print(f"🌊 Rivers with site matches: {rivers_with_sites:,}")

# ------------------------------------------------------------
# Write back
# ------------------------------------------------------------
with sqlite3.connect(db_path) as conn:
    flows_clean.to_sql(TABLE_FLOWS, conn, if_exists="replace", index=False)
    sites.to_sql(TABLE_USGS_RIVERNAMES, conn, if_exists="replace", index=False)

print(f"🔄 Updated [{TABLE_FLOWS}] in place")
print(f"🔄 Updated [{TABLE_USGS_RIVERNAMES}] in place")
print("🎉 Step 4 complete.")

