# 2_USGSsites.py
# ------------------------------------------------------------
# Step 2 (Flows): Extract *active-only* Washington USGS stations
# using NWIS API. Filters out any site_no longer than 10 digits.
#
# Output:
#   100_Data/2_USGSsites.csv     (snapshot)
#
# Columns:
#   raw_text
#   site_name
#   site_number
# ------------------------------------------------------------

import requests
import pandas as pd
from pathlib import Path

print("🌊 Step 2: Fetching ACTIVE Washington USGS site list (filtered)…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

output_path = data_dir / "2_USGSsites.csv"

# ------------------------------------------------------------
# API Endpoint for ACTIVE sites only
# ------------------------------------------------------------
URL = (
    "https://waterservices.usgs.gov/nwis/site/"
    "?format=rdb&stateCd=WA&siteStatus=active"
)

print(f"🌐 Downloading: {URL}")

try:
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
    text = resp.text.splitlines()
    print("📥 Successfully downloaded active WA station list.")
except Exception as e:
    raise RuntimeError(f"❌ Failed to download station list: {e}")

# ------------------------------------------------------------
# Parse RDB lines
# ------------------------------------------------------------
rows = []

def valid_site_number(s: str) -> bool:
    """Keep site numbers with length 8–10 AND numeric."""
    s = s.strip()
    return s.isdigit() and (8 <= len(s) <= 10)

for line in text:
    if not line or line.startswith("#"):
        continue

    parts = line.split("\t")
    if len(parts) < 3:
        continue

    agency = parts[0].strip()
    site_no = parts[1].strip()
    station_nm = parts[2].strip()

    if agency != "USGS":
        continue

    # --- FILTER: only site_no 8–10 digits ---
    if not valid_site_number(site_no):
        continue

    raw_text = f"{station_nm} - USGS-{site_no}"

    rows.append({
        "raw_text": raw_text,
        "site_name": station_nm,
        "site_number": site_no
    })

print(f"🔍 Parsed {len(rows):,} ACTIVE stations (filtered to 8–10 digit site_no).")

# ------------------------------------------------------------
# Save CSV snapshot
# ------------------------------------------------------------
df = pd.DataFrame(rows)
df.to_csv(output_path, index=False)

print(f"💾 Saved → {output_path}")
print("✅ Step 2 complete — Active USGS stations collected.")
