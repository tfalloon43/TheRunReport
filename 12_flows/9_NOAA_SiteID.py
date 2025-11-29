# 9_NOAA_siteID.py
# ------------------------------------------------------------
# Step 9: Extract NOAA "Site ID" from column "ID".
#
# Logic:
#   • Site ID = substring of ID from start → first "1" (inclusive)
#   • Example:
#        ID = "AHUW1PDTAHTANUM..." → Site ID = "AHUW1"
#        ID = "SHAW1SEWBAKER..."   → Site ID = "SHAW1"
#
# Input:
#   100_Data/csv_NOAA_completelist.csv
#
# Output:
#   100_Data/9_NOAA_siteID.csv    (snapshot)
#   csv_NOAA_completelist.csv     (updated in place)
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🔎 Step 9: Extracting NOAA Site ID from ID column...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root  = Path(__file__).resolve().parents[1]
data_dir      = project_root / "100_Data"

input_path    = data_dir / "csv_NOAA_completelist.csv"
output_path   = data_dir / "9_NOAA_siteID.csv"
recent_path   = input_path   # update original

# ------------------------------------------------------------
# Load
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_NOAA_completelist.csv")

# ------------------------------------------------------------
# Validate column
# ------------------------------------------------------------
if "ID" not in df.columns:
    raise ValueError("❌ 'ID' column not found in csv_NOAA_completelist.csv")

# ------------------------------------------------------------
# Extract Site ID
# ------------------------------------------------------------
def extract_site_id(full_id: str) -> str:
    """Return substring from start of ID → first '1' inclusive."""
    if not isinstance(full_id, str):
        return ""

    idx = full_id.find("1")
    if idx == -1:
        return ""  # No digit '1' found

    return full_id[: idx + 1]  # include the '1'

df["Site ID"] = df["ID"].astype(str).apply(extract_site_id)

print("🔧 Extracted Site ID column.")

# ------------------------------------------------------------
# Save
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"💾 Snapshot saved → {output_path}")
print(f"🔄 Updated csv_NOAA_completelist.csv in place")
print("✅ Step 9 complete — Site ID extraction done.")