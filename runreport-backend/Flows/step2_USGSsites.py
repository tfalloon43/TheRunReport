# 2_USGSsites.py
# ------------------------------------------------------------
# Step 2 (Flows): Extract *active-only* Washington USGS stations
# using NWIS API. Filters out any site_no longer than 10 digits.
#
# Output table (local.db):
#   - Flows_USGSsites
#
# Columns:
#   raw_text
#   site_name
#   site_number
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd
import requests

print("🌊 Step 2 (Flows): Fetching ACTIVE Washington USGS site list (filtered)…")

TABLE_USGS_SITES = "Flows_USGSsites"

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

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


def valid_site_number(site_number: str) -> bool:
    site_number = site_number.strip()
    return site_number.isdigit() and (8 <= len(site_number) <= 10)


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

    if not valid_site_number(site_no):
        continue

    raw_text = f"{station_nm} - USGS-{site_no}"

    rows.append(
        {
            "raw_text": raw_text,
            "site_name": station_nm,
            "site_number": site_no,
        }
    )

print(f"🔍 Parsed {len(rows):,} ACTIVE stations (filtered to 8–10 digit site_no).")

df = pd.DataFrame(rows)

with sqlite3.connect(db_path) as conn:
    df.to_sql(TABLE_USGS_SITES, conn, if_exists="replace", index=False)

print(f"💾 Saved {len(df):,} rows → table [{TABLE_USGS_SITES}]")
print("✅ Step 2 complete — Active USGS stations collected into local.db.")

