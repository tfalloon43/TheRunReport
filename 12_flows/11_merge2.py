# 11_merge2.py
# ------------------------------------------------------------
# Correct NOAA merge that APPENDS Site/Gage pairs without
# deleting existing USGS data.
#
# flow_presence is NO LONGER TOUCHED in THIS SCRIPT.
#
# Behavior:
#   • Detect last used Site n per river
#   • Append NOAA Site n+1 / Gage #(n+1)
#   • Preserve existing USGS data
#   • Mark NOAA rows as "added" inside 6_NOAAsites.csv
#   • NEVER modify flow_presence
#
# Outputs:
#   • 11_NOAAmerge_output_flows.csv
#   • 11_NOAAmerge_output_sites.csv
#   • updates flows.csv and 6_NOAAsites.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
import re

print("🌧️ Step 11: APPENDING NOAA gauge info into flows.csv (non-destructive)…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

flows_path = data_dir / "flows.csv"
noaa_path  = data_dir / "6_NOAAsites.csv"

flows_out  = data_dir / "11_NOAAmerge_output_flows.csv"
sites_out  = data_dir / "11_NOAAmerge_output_sites.csv"

recent_flows = flows_path
recent_sites = noaa_path

# ------------------------------------------------------------
# Load datasets
# ------------------------------------------------------------
flows = pd.read_csv(flows_path, dtype=str).fillna("")
noaa  = pd.read_csv(noaa_path, dtype=str).fillna("")

print(f"📘 Loaded {len(flows)} rivers from flows.csv")
print(f"📘 Loaded {len(noaa)} NOAA entries")

# Ensure "added" column exists
if "added" not in noaa.columns:
    noaa["added"] = ""

# ------------------------------------------------------------
# Helper: find next available Site index
# ------------------------------------------------------------
def next_site_index(row):
    """Find next free Site n number for this row."""
    pattern = re.compile(r"Site (\d+)$")
    nums = []

    for col in flows.columns:
        m = pattern.match(col)
        if m:
            n = int(m.group(1))
            val = str(row.get(col, "")).strip()
            if val:
                nums.append(n)

    return max(nums) + 1 if nums else 1

# ------------------------------------------------------------
# Merge NOAA data into flows.csv
# ------------------------------------------------------------
total_added = 0
rivers_updated = 0

for idx, row in flows.iterrows():

    river = str(row["river"]).strip().lower()

    # Match NOAA rows for this river
    match = noaa[noaa["river"].str.lower() == river]

    if match.empty:
        continue

    rivers_updated += 1
    start_index = next_site_index(row)
    local_i = 0

    for _, r in match.iterrows():

        # Iterate NOAA dynamic Site n fields
        k = 1
        while f"Site {k}" in r:

            site_val = r.get(f"Site {k}", "").strip()
            gage_val = r.get(f"Gage #{k}", "").strip()

            if site_val and gage_val:
                flows.at[idx, f"Site {start_index + local_i}"] = site_val
                flows.at[idx, f"Gage #{start_index + local_i}"] = gage_val
                local_i += 1
                total_added += 1

            k += 1
            if k > 50:  # safety
                break

    # DO NOT MODIFY flow_presence HERE

    # Mark NOAA rows
    noaa.loc[match.index, "added"] = "Y"

print(f"🌧️ NOAA sites appended: {total_added}")
print(f"📍 Rivers updated with NOAA data: {rivers_updated}")

# ------------------------------------------------------------
# Clean NaN → empty strings
# ------------------------------------------------------------
flows = flows.fillna("")
noaa  = noaa.fillna("")

# ------------------------------------------------------------
# Save CSVs with no NaN
# ------------------------------------------------------------
flows.to_csv(flows_out, index=False, na_rep="")
flows.to_csv(recent_flows, index=False, na_rep="")

noaa.to_csv(sites_out, index=False, na_rep="")
noaa.to_csv(recent_sites, index=False, na_rep="")

print(f"💾 flows snapshot → {flows_out}")
print(f"💾 NOAA site snapshot → {sites_out}")
print("🎉 Step 11 complete — NOAA appended, flow_presence untouched.")