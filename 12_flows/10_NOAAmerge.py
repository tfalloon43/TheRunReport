# 10_NOAAmerge.py
# ------------------------------------------------------------
# Merge NOAA station info from csv_NOAA_completelist.csv
# into 6_NOAAsites.csv using cleaned river names.
#
# Matching logic:
#   • 6_NOAAsites.csv:  column "river"
#   • csv_NOAA_completelist.csv: column "river_name"
#
# Outputs:
#   • 100_Data/10_NOAAmerge_output.csv  (snapshot)
#   • 100_Data/6_NOAAsites.csv          (updated in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
import re

print("🌧️ Step 10: Merging NOAA station info into 6_NOAAsites.csv...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

sites_path   = data_dir / "6_NOAAsites.csv"
catalog_path = data_dir / "csv_NOAA_completelist.csv"

output_path  = data_dir / "10_NOAAmerge_output.csv"
recent_path  = sites_path   # update 6_NOAAsites.csv in place

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not sites_path.exists():
    raise FileNotFoundError(f"❌ 6_NOAAsites.csv not found at: {sites_path}")

if not catalog_path.exists():
    raise FileNotFoundError(f"❌ csv_NOAA_completelist.csv not found at: {catalog_path}")

sites_df   = pd.read_csv(sites_path)
catalog_df = pd.read_csv(catalog_path)

print(f"📘 Loaded {len(sites_df):,} rows from 6_NOAAsites.csv")
print(f"📘 Loaded {len(catalog_df):,} rows from csv_NOAA_completelist.csv")

# ------------------------------------------------------------
# Validate columns
# ------------------------------------------------------------
if "Description" not in catalog_df.columns:
    raise ValueError("❌ csv_NOAA_completelist.csv must contain a 'Description' column.")

if "Site ID" not in catalog_df.columns:
    raise ValueError("❌ csv_NOAA_completelist.csv must contain a 'Site ID' column.")

# ------------------------------------------------------------
# Extract or clean river_name from NOAA Description
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# Normalization for matching
# ------------------------------------------------------------
def normalize_name(x: str) -> str:
    if not isinstance(x, str):
        return ""
    x = x.lower().strip()

    remove_words = [
        "river", "creek", "fork",
        "north fork", "south fork", "east fork", "west fork",
        "n fork", "s fork", "e fork", "w fork"
    ]
    for w in remove_words:
        x = re.sub(rf"\b{w}\b", "", x)

    x = re.sub(r"[^a-z0-9\s]", " ", x)
    x = re.sub(r"\s+", " ", x)
    return x.strip()

sites_df["river_norm"]   = sites_df["river"].astype(str).apply(normalize_name)
catalog_df["river_norm"] = catalog_df["river_name"].astype(str).apply(normalize_name)

print("🔍 Normalized river names for matching")

# ------------------------------------------------------------
# Perform matching for each river
# ------------------------------------------------------------
pairs_per_row = {}
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
        contains = catalog_df[
            catalog_df["river_norm"].str.contains(target, na=False)
            | catalog_df["river_norm"].apply(lambda v: target in v if isinstance(v, str) else False)
        ]
        matches = contains
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
        sid  = str(m["Site ID"]).strip()
        if desc and sid:
            collected.append((desc, sid))

    pairs_per_row[idx] = collected

print(f"✅ Rivers matched with NOAA stations: {match_count}")
print(f"⚠️ Rivers without NOAA match: {no_match_count}")

# ------------------------------------------------------------
# Determine max number of stations and create columns
# ------------------------------------------------------------
max_pairs = max((len(v) for v in pairs_per_row.values()), default=0)
print(f"🔢 Maximum stations for any river: {max_pairs}")

for n in range(1, max_pairs + 1):
    sites_df[f"Site {n}"]  = ""
    sites_df[f"Gage #{n}"] = ""

# Fill columns
for idx, pairs in pairs_per_row.items():
    for j, (desc, sid) in enumerate(pairs, start=1):
        sites_df.at[idx, f"Site {j}"]  = desc
        sites_df.at[idx, f"Gage #{j}"] = sid

print("🧩 NOAA station columns populated")

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
sites_df.to_csv(output_path, index=False)
sites_df.to_csv(recent_path, index=False)

print(f"💾 Snapshot saved → {output_path}")
print(f"🔄 6_NOAAsites.csv updated in place")
print("🎉 Step 10 (NOAA merge) complete!")