# 7_NOAA_completelist.py
# ------------------------------------------------------------
# Step 7 (Flows): Scrape NOAA NWRFC full river gauge table from:
#   https://www.nwrfc.noaa.gov/river/river_summary.php
#
# Output table:
#   - Flows_NOAA_completelist
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

print("🌧️ Step 7 (Flows): Downloading full NOAA NWRFC river gauge table…")

TABLE_NOAA_CATALOG = "Flows_NOAA_completelist"

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# Fetch NOAA page
# ------------------------------------------------------------
URL = "https://www.nwrfc.noaa.gov/river/river_summary.php"
print(f"🌐 Fetching NOAA river summary page:\n{URL}")

try:
    resp = requests.get(URL, timeout=20)
    resp.raise_for_status()
except Exception as e:
    raise RuntimeError(f"❌ Failed to download NOAA page: {e}")

soup = BeautifulSoup(resp.text, "html.parser")

tables = soup.find_all("table")
if len(tables) < 2:
    raise RuntimeError("❌ Could not find NOAA station table (#2).")

station_table = tables[1]
rows = station_table.find_all("tr")
print(f"📡 Rows detected (including header): {len(rows)}")

if len(rows) <= 1:
    raise RuntimeError("❌ NOAA station table appears empty.")

header_cells = rows[0].find_all(["th", "td"])
headers = [cell.get_text(strip=True) for cell in header_cells]

data = []
for tr in rows[1:]:
    cells = tr.find_all("td")
    if len(cells) != len(headers):
        continue
    data.append([c.get_text(strip=True) for c in cells])

print(f"📊 Parsed {len(data):,} NOAA gauge stations.")

df = pd.DataFrame(data, columns=headers)

if "Description" in df.columns:
    df["river_name"] = df["Description"].apply(
        lambda x: x.split("-")[0].strip() if isinstance(x, str) else ""
    )

with sqlite3.connect(db_path) as conn:
    df.to_sql(TABLE_NOAA_CATALOG, conn, if_exists="replace", index=False)

print(f"💾 Saved NOAA complete station list → table [{TABLE_NOAA_CATALOG}]")
print("✅ Step 7 complete.")

