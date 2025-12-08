"""
1_datapull.py — IN-MEMORY VERSION (updated for unified DB)

Fetches Columbia/Snake River daily adult counts from FPC.

This version:
  - Downloads CSVs via POST
  - Parses into DataFrames
  - Adds metadata columns
  - RETURNS a combined DataFrame (no CSV files written)
  - When run directly, writes to runreport-backend/0_db/local.db
"""

import re
import io
import requests
import pandas as pd
from typing import Optional
from pathlib import Path
import sys

# -------------------------------------------------------------------
# Folder Layout (IMPORTANT)
#
#   runreport-backend/
#       0_db/
#           local.db
#           sqlite_manager.py
#       Columbia_FishCounts/
#           1_datapull.py  <— this file
# -------------------------------------------------------------------

CURRENT_DIR = Path(__file__).resolve().parent          # Columbia_FishCounts/
BACKEND_ROOT = CURRENT_DIR.parent                     # runreport-backend/
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

# Ensure 0_db is importable
sys.path.append(str(DB_DIR))

from sqlite_manager import SQLiteManager


# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

BASE_URL = "https://www.fpc.org"
RESULT_URL = f"{BASE_URL}/adults/R_dailyadultcountsgraph_resultsV6.php"

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/18.1.1 Safari/605.1.15"
    ),
    "Referer": f"{BASE_URL}/adults/Q_dailyadultcountsgraph2.php",
    "Origin": BASE_URL,
    "Content-Type": "application/x-www-form-urlencoded",
}

DAM_CODES = {
    "BON": "Bonneville Dam",
    "TDA": "The Dalles Dam",
    "JDA": "John Day Dam",
    "MCN": "McNary Dam",
    "IHR": "Ice Harbor Dam",
    "LMN": "Lower Monumental Dam",
    "LGS": "Little Goose Dam",
    "LGR": "Lower Granite Dam",
    "PRD": "Priest Rapids Dam",
    "WAN": "Wanapum Dam",
    "RIS": "Rock Island Dam",
    "RRH": "Rocky Reach Dam",
    "WEL": "Wells Dam",
}

SPECIES_CODES = {
    "CHAD": "Chinook Adult",
    "COAD": "Coho Adult",
    "ST": "Steelhead",
    "SO": "Sockeye",
}

DAM_SPECIES_PAIRS = [(d, s) for d in DAM_CODES for s in SPECIES_CODES]

CSV_REGEX = re.compile(r'/DataReqs/web/apps/adultsalmon/[^"]+\.csv')


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def fetch_results_html(session: requests.Session, dam_code: str, species_code: str) -> Optional[str]:
    """POST to the results page, return HTML."""
    try:
        resp = session.post(
            RESULT_URL,
            headers=COMMON_HEADERS,
            data={"dam": dam_code, "species": species_code},
            timeout=30,
        )
        return resp.text if resp.status_code == 200 else None
    except Exception:
        return None


def extract_csv_url(html: str) -> Optional[str]:
    """Find CSV link inside the HTML."""
    m = CSV_REGEX.search(html)
    if not m:
        return None
    path = m.group(0)
    if not path.startswith("/"):
        path = "/" + path
    return BASE_URL + path


def download_csv(session: requests.Session, url: str) -> Optional[str]:
    """Download raw CSV text."""
    try:
        resp = session.get(url, headers={"User-Agent": COMMON_HEADERS["User-Agent"]}, timeout=30)
        return resp.text if resp.status_code == 200 else None
    except Exception:
        return None


# -------------------------------------------------------------------
# MAIN DATA FETCH FUNCTION
# -------------------------------------------------------------------

def fetch_columbia_daily() -> pd.DataFrame:
    """Fetch all dam × species CSVs into one combined DataFrame."""
    all_frames = []

    with requests.Session() as session:
        for dam_code, species_code in DAM_SPECIES_PAIRS:
            print(f"Fetching {dam_code}/{species_code}...")

            html = fetch_results_html(session, dam_code, species_code)
            if not html:
                continue

            csv_url = extract_csv_url(html)
            if not csv_url:
                continue

            csv_text = download_csv(session, csv_url)
            if not csv_text:
                continue

            try:
                df = pd.read_csv(io.StringIO(csv_text))
            except Exception as e:
                print(f"❌ Error parsing CSV for {dam_code}/{species_code}: {e}")
                continue

            df["dam_code"] = dam_code
            df["dam_name"] = DAM_CODES[dam_code]
            df["species_code"] = species_code
            df["species_name"] = SPECIES_CODES[species_code]

            all_frames.append(df)

    if not all_frames:
        raise RuntimeError("❌ No data collected for any dam/species!")

    return pd.concat(all_frames, ignore_index=True)


# -------------------------------------------------------------------
# Run directly → write to SQLite
# -------------------------------------------------------------------

if __name__ == "__main__":

    print("📥 Running 1_datapull locally...")
    df = fetch_columbia_daily()
    print(f"✔ Downloaded {len(df):,} rows total.")

    # Write to unified local.db
    db = SQLiteManager(DB_PATH)
    db.write_df("Columbia_FishCounts", df)

    print(f"✔ Wrote data to {DB_PATH} in table Columbia_FishCounts.")