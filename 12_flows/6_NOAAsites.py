# 6_NOAAsites.py
# ------------------------------------------------------------
# Step 6 (Flows): Identify rivers that do NOT yet have USGS
# flow coverage (flow_presence is blank) and prepare them
# for potential NOAA handling later.
#
# For now this script:
#   • Reads 100_Data/flows.csv
#   • Finds rows where flow_presence is blank/NaN
#   • Applies a manual exclusion list (rearing ponds, etc.)
#   • Prints the final list of “missing-flow” rivers
#   • Saves that list to 100_Data/6_NOAAsites_missing_rivers.csv
#
# We are *not* calling a NOAA API here yet, because their
# public endpoints have been flaky / undocumented. This
# step is just about getting the correct river list.
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🌧️ Step 6: Finding rivers without USGS coverage (for future NOAA gauges)…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

flows_path   = data_dir / "flows.csv"
missing_out  = data_dir / "6_NOAAsites.csv"

# ------------------------------------------------------------
# Load flows.csv
# ------------------------------------------------------------
if not flows_path.exists():
    raise FileNotFoundError(f"❌ flows.csv not found at: {flows_path}")

flows = pd.read_csv(flows_path)
print(f"📘 Loaded {len(flows):,} rows from flows.csv")

# Ensure expected columns exist
if "river" not in flows.columns:
    raise ValueError("❌ flows.csv is missing a 'river' column.")
if "flow_presence" not in flows.columns:
    raise ValueError("❌ flows.csv is missing a 'flow_presence' column.")

# ------------------------------------------------------------
# Detect “missing-flow” rivers
#   EXACTLY like your working blank.py
# ------------------------------------------------------------
fp = flows["flow_presence"]

missing_mask = (
    fp.isna() |
    (fp.astype(str).str.strip() == "") |
    (fp.astype(str).str.lower().isin(["nan", "none"]))
)

missing_rivers_raw = flows.loc[missing_mask, "river"].astype(str).str.strip().tolist()
print(f"🔍 Raw missing-flow rivers (before manual exclusions): {missing_rivers_raw}")

# ------------------------------------------------------------
# Manual exclusions (rearing ponds / non-flow systems)
# ------------------------------------------------------------
MANUAL_SKIP = {
    # add any you never want to send to NOAA here
    "Cottonwood Creek Pond",
    "Orcas Island",
    "Hood Canal",
    "Kalama River",

}

missing_rivers = [r for r in missing_rivers_raw if r not in MANUAL_SKIP]

print(f"⏭️ After skipping manual exclusions: {missing_rivers}")

# ------------------------------------------------------------
# Save snapshot CSV of missing-flow rivers
# ------------------------------------------------------------
missing_df = pd.DataFrame({"river": sorted(missing_rivers)})
missing_df.to_csv(missing_out, index=False)

print(f"💾 Saved list of rivers needing flow sources → {missing_out}")
print("✅ Step 6 complete — missing-flow rivers correctly identified.")