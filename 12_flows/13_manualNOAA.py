# 13_manualNOAA.py
# ------------------------------------------------------------
# Manually add NOAA (or custom) gauge info into flows.csv.
#
# You define:
#   • flow_presence (EXACT value you want to insert)
#   • site_name
#   • gage
#
# Behavior:
#   • Matches river in flows.csv (case-insensitive)
#   • Adds Site n / Gage #n at the next available slot
#   • Overwrites flow_presence with whatever YOU specify
#
# Output:
#   • 13_manualNOAA_output.csv  (snapshot)
#   • flows.csv updated in place
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
import re

print("📝 Step 13: Manually inserting NOAA/custom gauge data into flows.csv…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

flows_path = data_dir / "flows.csv"
output_path = data_dir / "13_manualNOAA_output.csv"

# ------------------------------------------------------------
# Load flows.csv
# ------------------------------------------------------------
if not flows_path.exists():
    raise FileNotFoundError(f"❌ flows.csv not found at: {flows_path}")

df = pd.read_csv(flows_path, dtype=str).fillna("")
print(f"📘 Loaded {len(df):,} rows from flows.csv")

# ------------------------------------------------------------
# MANUAL ENTRIES — YOU FILL THESE IN
#
# MUST include:
#   "flow_presence"
#   "site_name"
#   "gage"
#
# Below are 3 skeleton entries ready for your data.
# ------------------------------------------------------------
manual_entries = {
     "Elochoman River": {
         "flow_presence": "NOAA",
         "site_name": "Elochoman River above Cathlamet",
         "gage": "ECHW1",
     },

    # Skeleton 1:
    #"": {
    #    "flow_presence": "",
    #    "site_name": "",
    #    "gage": "",
    #},

    # Skeleton 2:
    #"": {
    #    "flow_presence": "",
    #    "site_name": "",
    #    "gage": "",
    #},

    # Skeleton 3:
    #"": {
    #    "flow_presence": "",
    #    "site_name": "",
    #    "gage": "",
    #},
}

# Remove blank keys (so skeletons don't cause errors)
manual_entries = {
    k: v for k, v in manual_entries.items() if k.strip() != ""
}

# ------------------------------------------------------------
# Helper: determine next available Site index
# ------------------------------------------------------------
def next_site_index(row):
    pattern = re.compile(r"Site (\d+)")
    indices = []

    for col in df.columns:
        m = pattern.match(col)
        if m:
            n = int(m.group(1))
            if row.get(col, "").strip():
                indices.append(n)

    return max(indices) + 1 if indices else 1


# ------------------------------------------------------------
# APPLY MANUAL ENTRIES
# ------------------------------------------------------------
applied = 0
rivers_found = 0

for river_name, info in manual_entries.items():

    desired_fp = info["flow_presence"].strip()
    site_name  = info["site_name"].strip()
    gage_code  = info["gage"].strip()

    match = df[df["river"].str.lower() == river_name.lower()]

    if match.empty:
        print(f"⚠️ River not found in flows.csv → {river_name}")
        continue

    rivers_found += 1

    for idx in match.index:

        # Determine next available Site/Gage column
        i = next_site_index(df.loc[idx])

        df.at[idx, f"Site {i}"] = site_name
        df.at[idx, f"Gage #{i}"] = gage_code
        applied += 1

        # Set flow_presence EXACTLY to what user typed
        if desired_fp:
            df.at[idx, "flow_presence"] = desired_fp

print(f"🌊 Manual river updates applied: {rivers_found}")
print(f"➕ Total new Site/Gage entries inserted: {applied}")

# ------------------------------------------------------------
# SAVE OUTPUTS
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(flows_path, index=False)

print(f"💾 Snapshot saved → {output_path}")
print(f"🔄 flows.csv updated in place")
print("✅ Step 13 complete.")