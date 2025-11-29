# 3_rivername.py
# ------------------------------------------------------------
# Step 3 (Flows): Extract canonical river/creek name from
# USGS site_name strings.
#
# Input : 100_Data/2_USGSsites.csv
# Output:
#   • 100_Data/3_rivername.csv
#   • Updates flows.csv IN PLACE
#
# Logic:
#   Extract everything up to and including:
#     - "river"
#     - "creek"
#     - "fork" (only if followed by river/creek)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
import re

print("🌊 Step 3: Extracting standardized river_name from site_name…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path   = data_dir / "2_USGSsites.csv"
output_path  = data_dir / "3_rivername.csv"
flows_path   = data_dir / "flows.csv"   # updated in place if exists

# ------------------------------------------------------------
# Load input
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📘 Loaded {len(df):,} USGS sites from 2_USGSsites.csv")

# ------------------------------------------------------------
# Helper: Extract river/creek name
# ------------------------------------------------------------
def extract_river_name(name: str) -> str:
    if not isinstance(name, str):
        return ""

    text = name.lower()

    # Patterns to match
    patterns = [
        r"(.+?river)\b",
        r"(.+?creek)\b",
        r"(.+?fork.+?river)\b",
        r"(.+?fork.+?creek)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            extracted = match.group(1)
            # Clean + Title-case
            extracted = extracted.replace(",", "").strip()
            return extracted.title()

    # Fallback — use the whole name before comma
    base = name.split(",")[0].strip()
    return base.title()

# ------------------------------------------------------------
# Apply extraction
# ------------------------------------------------------------
df["river_name"] = df["site_name"].apply(extract_river_name)

print("🔍 Extracted river_name for all stations.")

# ------------------------------------------------------------
# Save snapshot
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
print(f"💾 Saved → {output_path}")

# ------------------------------------------------------------
# Update flows.csv in place (if present)
# ------------------------------------------------------------
if flows_path.exists():
    flows = pd.read_csv(flows_path)
    print(f"📘 Updating flows.csv with river_name matches…")

    # For each river in flows, find matching stations by substring
    def match_river(river):
        river_lower = river.lower()
        matches = df[df["river_name"].str.lower().str.contains(river_lower)]
        return ", ".join(sorted(matches["river_name"].unique()))

    flows["river_name"] = flows["river"].astype(str).apply(match_river)

    flows.to_csv(flows_path, index=False)
    print(f"🔄 flows.csv updated with river_name column.")

print("✅ Step 3 complete — river names extracted and flows updated.")
