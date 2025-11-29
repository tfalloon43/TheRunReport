# 4_merge1.py (CLEAN VERSION — added column now stored in 3_rivername.csv)
# ------------------------------------------------------------
# Step 4: Merge USGS site info (site_name + site_number) INTO flows.csv,
#         AND mark "added" inside 3_rivername.csv instead of flows.csv.
#
# Creates:
#   • 4_merge1_output.csv            (snapshot of flows.csv)
#   • 4_merge1_sites_output.csv      (snapshot of updated 3_rivername.csv)
#   • updates flows.csv in place
#   • updates 3_rivername.csv in place
#
# Behavior:
#   • For each river in flows.csv:
#         - match river_name (case-insensitive)
#         - collect ALL (site_name, site_number)
#         - rewrite clean numbered columns:
#               Site 1 | Gage #1 | Site 2 | Gage #2 | ...
#   • For each matched site in 3_rivername.csv:
#         - mark "added" = "Y"
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🔗 Step 4: Merging USGS site info INTO flows.csv (clean & renumbered)...")
print("📝 'added' flags will be stored in 3_rivername.csv only.")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

flows_path   = data_dir / "flows.csv"
sites_path   = data_dir / "3_rivername.csv"

flows_out    = data_dir / "4_merge1_output.csv"
sites_out    = data_dir / "4_merge1_sites_output.csv"

recent_flows = flows_path
recent_sites = sites_path

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
flows = pd.read_csv(flows_path)
sites = pd.read_csv(sites_path)

print(f"📘 Loaded {len(flows):,} rivers from flows.csv")
print(f"📘 Loaded {len(sites):,} USGS sites from 3_rivername.csv")

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
required_flows = ["river"]
required_sites = ["river_name", "site_name", "site_number"]

for col in required_flows:
    if col not in flows.columns:
        raise ValueError(f"❌ flows.csv missing required column: {col}")

for col in required_sites:
    if col not in sites.columns:
        raise ValueError(f"❌ 3_rivername.csv missing required column: {col}")

# ------------------------------------------------------------
# Prepare sites file: add "added" if not existing
# ------------------------------------------------------------
if "added" not in sites.columns:
    sites["added"] = ""

# Normalize site_number into clean strings
def clean_site_no(x):
    try:
        return str(int(float(x))).strip()
    except:
        return ""

sites["site_number_clean"] = sites["site_number"].apply(clean_site_no)

# ------------------------------------------------------------
# Prepare flows: remove all old Site n / Gage #n columns
# ------------------------------------------------------------
site_cols = [c for c in flows.columns if c.lower().startswith("site ")]
gage_cols = [c for c in flows.columns if c.lower().startswith("gage #")]

flows_clean = flows.drop(columns=site_cols + gage_cols, errors="ignore").copy()

print(f"🧹 Removed {len(site_cols)+len(gage_cols)} old site/gage columns from flows.csv")

# ------------------------------------------------------------
# Merge logic
# ------------------------------------------------------------
total_added = 0
rivers_with_sites = 0

for idx, row in flows_clean.iterrows():
    river = str(row["river"]).strip().lower()

    # Find matching USGS rows
    matched = sites[sites["river_name"].str.lower() == river]

    if matched.empty:
        continue

    rivers_with_sites += 1

    # Collect site pairs
    site_pairs = list(zip(
        matched["site_name"].tolist(),
        matched["site_number_clean"].tolist()
    ))

    # Assign new numbering starting at 1 per river
    for i, (sname, snum) in enumerate(site_pairs, start=1):
        flows_clean.at[idx, f"Site {i}"] = sname
        flows_clean.at[idx, f"Gage #{i}"] = snum
        total_added += 1

    # Mark these rows in 3_rivername.csv
    sites.loc[matched.index, "added"] = "Y"

print(f"🧩 Added {total_added:,} site/gage entries")
print(f"🌊 Rivers with site matches: {rivers_with_sites:,}")

# ------------------------------------------------------------
# Save updated files
# ------------------------------------------------------------
flows_clean.to_csv(flows_out, index=False)
sites.to_csv(sites_out, index=False)

flows_clean.to_csv(recent_flows, index=False)
sites.to_csv(recent_sites, index=False)

print(f"💾 Saved flows snapshot → {flows_out}")
print(f"💾 Saved updated USGS site snapshot → {sites_out}")
print(f"🔄 flows.csv updated in place")
print(f"🔄 3_rivername.csv updated in place")
print("🎉 Step 4 complete.")
