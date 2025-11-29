# 12_flowpresence2.py
# ------------------------------------------------------------
# Update flow_presence ONLY when:
#   • flow_presence is blank AND
#   • "Site 1" contains ANY value
#
# Then → flow_presence = "NOAA"
#
# Output:
#   • 12_flowpresence2_output.csv (snapshot)
#   • flows.csv updated in place
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("💧 Step 12: Updating flow_presence where appropriate (NOAA only)…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

flows_path   = data_dir / "flows.csv"
output_path  = data_dir / "12_flowpresence2_output.csv"

# ------------------------------------------------------------
# Load flows.csv
# ------------------------------------------------------------
if not flows_path.exists():
    raise FileNotFoundError(f"❌ flows.csv not found: {flows_path}")

df = pd.read_csv(flows_path, dtype=str).fillna("")
print(f"📘 Loaded {len(df):,} rows from flows.csv")

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
if "flow_presence" not in df.columns:
    raise ValueError("❌ flows.csv missing required column: flow_presence")

if "Site 1" not in df.columns:
    raise ValueError("❌ flows.csv missing required column: Site 1")

# ------------------------------------------------------------
# Update flow_presence
# ------------------------------------------------------------
updates = 0

for idx, row in df.iterrows():
    fp = str(row["flow_presence"]).strip()
    site1 = str(row["Site 1"]).strip()

    if fp == "" and site1 != "":
        df.at[idx, "flow_presence"] = "NOAA"
        updates += 1

print(f"💧 Updated {updates} rows with flow_presence = 'NOAA'")

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(flows_path, index=False)

print(f"💾 Snapshot saved → {output_path}")
print("🔄 flows.csv updated in place")
print("✅ Step 12 complete.")
