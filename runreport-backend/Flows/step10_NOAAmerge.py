# 10_NOAAmerge.py
# ------------------------------------------------------------
# Step 10 (Flows): Merge NOAA station info from Flows_NOAA_completelist
# into Flows_NOAAsites using cleaned river-name matching.
#
# Input tables:
#   - Flows_NOAAsites (column: river)
#   - Flows_NOAA_completelist (columns: Description, Site ID, river_name)
#
# Output table (overwritten):
#   - Flows_NOAAsites (adds Site n / Gage #n columns)
# ------------------------------------------------------------

import re
import sqlite3
from pathlib import Path

import pandas as pd

print("🌧️ Step 10 (Flows): Merging NOAA station info into Flows_NOAAsites...")

TABLE_NOAA_SITES = "Flows_NOAAsites"
TABLE_NOAA_CATALOG = "Flows_NOAA_completelist"

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

with sqlite3.connect(db_path) as conn:
    try:
        sites_df = pd.read_sql_query(f"SELECT * FROM [{TABLE_NOAA_SITES}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_NOAA_SITES}] in local.db: {e}")

    try:
        catalog_df = pd.read_sql_query(f"SELECT * FROM [{TABLE_NOAA_CATALOG}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_NOAA_CATALOG}] in local.db: {e}")

print(f"📘 Loaded {len(sites_df):,} rows from [{TABLE_NOAA_SITES}]")
print(f"📘 Loaded {len(catalog_df):,} rows from [{TABLE_NOAA_CATALOG}]")

if "Description" not in catalog_df.columns:
    raise ValueError(f"❌ [{TABLE_NOAA_CATALOG}] must contain a 'Description' column.")
if "Site ID" not in catalog_df.columns:
    raise ValueError(f"❌ [{TABLE_NOAA_CATALOG}] must contain a 'Site ID' column.")
if "river" not in sites_df.columns:
    raise ValueError(f"❌ [{TABLE_NOAA_SITES}] must contain a 'river' column.")


def extract_river_name(desc: str) -> str:
    if not isinstance(desc, str):
        return ""
    base = desc.split(" - ")[0]
    base = base.split(",")[0]
    return base.strip()


if "river_name" not in catalog_df.columns:
    catalog_df["river_name"] = catalog_df["Description"].apply(extract_river_name)
else:
    catalog_df["river_name"] = catalog_df["river_name"].astype(str).apply(extract_river_name)


def normalize_name(x: str) -> str:
    if not isinstance(x, str):
        return ""
    x = x.lower().strip()

    remove_words = [
        "river",
        "creek",
        "fork",
        "north fork",
        "south fork",
        "east fork",
        "west fork",
        "n fork",
        "s fork",
        "e fork",
        "w fork",
    ]
    for w in remove_words:
        x = re.sub(rf"\b{w}\b", "", x)

    x = re.sub(r"[^a-z0-9\s]", " ", x)
    x = re.sub(r"\s+", " ", x)
    return x.strip()


sites_df["river_norm"] = sites_df["river"].astype(str).apply(normalize_name)
catalog_df["river_norm"] = catalog_df["river_name"].astype(str).apply(normalize_name)

pairs_per_row: dict[int, list[tuple[str, str]]] = {}
no_match_count = 0
match_count = 0

for idx, row in sites_df.iterrows():
    target = row["river_norm"]
    if not target:
        pairs_per_row[idx] = []
        no_match_count += 1
        continue

    exact = catalog_df[catalog_df["river_norm"] == target]
    if exact.empty:
        matches = catalog_df[catalog_df["river_norm"].str.contains(target, na=False)]
    else:
        matches = exact

    if matches.empty:
        pairs_per_row[idx] = []
        no_match_count += 1
        continue

    match_count += 1

    collected = []
    for _, m in matches.iterrows():
        desc = str(m["Description"]).strip()
        sid = str(m["Site ID"]).strip()
        if desc and sid:
            collected.append((desc, sid))

    pairs_per_row[idx] = collected

print(f"✅ Rivers matched with NOAA stations: {match_count}")
print(f"⚠️ Rivers without NOAA match: {no_match_count}")

max_pairs = max((len(v) for v in pairs_per_row.values()), default=0)
print(f"🔢 Maximum stations for any river: {max_pairs}")

for n in range(1, max_pairs + 1):
    sites_df[f"Site {n}"] = ""
    sites_df[f"Gage #{n}"] = ""

for idx, pairs in pairs_per_row.items():
    for j, (desc, sid) in enumerate(pairs, start=1):
        sites_df.at[idx, f"Site {j}"] = desc
        sites_df.at[idx, f"Gage #{j}"] = sid

sites_df = sites_df.drop(columns=["river_norm"], errors="ignore")

with sqlite3.connect(db_path) as conn:
    sites_df.to_sql(TABLE_NOAA_SITES, conn, if_exists="replace", index=False)

print(f"🔄 Updated [{TABLE_NOAA_SITES}] in place")
print("🎉 Step 10 complete.")
