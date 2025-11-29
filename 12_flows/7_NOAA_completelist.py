# 7_NOAA_completelist.py
# ------------------------------------------------------------
# Scrape NOAA NWRFC full river gauge table from:
#   https://www.nwrfc.noaa.gov/river/river_summary.php
#
# Saves:
#   100_Data/csv_NOAA_completelist.csv
#
# Table includes:
#   ID, HSA, Description, State, County, Elevation,
#   Observation Time, Stage, Discharge, Status
# ------------------------------------------------------------

import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

print("🌧️ Step 7: Downloading full NOAA NWRFC river gauge table…")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

output_path = data_dir / "csv_NOAA_completelist.csv"

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

# ------------------------------------------------------------
# Extract tables
# ------------------------------------------------------------
tables = soup.find_all("table")
if len(tables) < 2:
    raise RuntimeError("❌ Could not find NOAA station table (#2).")

station_table = tables[1]  # second table contains all stations
rows = station_table.find_all("tr")

print(f"📡 Rows detected (including header): {len(rows)}")

if len(rows) <= 1:
    raise RuntimeError("❌ NOAA station table appears empty.")

# ------------------------------------------------------------
# Parse header
# ------------------------------------------------------------
header_cells = rows[0].find_all(["th", "td"])
headers = [cell.get_text(strip=True) for cell in header_cells]

print(f"📝 Detected {len(headers)} columns:")
for h in headers:
    print(f"   • {h}")

# ------------------------------------------------------------
# Parse data rows
# ------------------------------------------------------------
data = []
for tr in rows[1:]:  # skip header
    cells = tr.find_all("td")
    if len(cells) != len(headers):
        # skip malformed rows
        continue
    row = [c.get_text(strip=True) for c in cells]
    data.append(row)

print(f"📊 Parsed {len(data):,} NOAA gauge stations.")

# ------------------------------------------------------------
# Convert to DataFrame
# ------------------------------------------------------------
df = pd.DataFrame(data, columns=headers)

# Add river_name extracted from Description
if "Description" in df.columns:
    df["river_name"] = df["Description"].apply(
        lambda x: x.split("-")[0].strip() if isinstance(x, str) else ""
    )

# ------------------------------------------------------------
# Save CSV
# ------------------------------------------------------------
df.to_csv(output_path, index=False)

print(f"💾 Saved NOAA complete station list → {output_path}")
print("✅ Step 7 complete — csv_NOAA_completelist.csv created.")