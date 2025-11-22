#!/usr/bin/env python3
"""
1_datapull.py

Pull Columbia/Snake River daily adult counts from FPC by:
  - POSTing directly to the results endpoint with dam/species codes
  - Extracting the CSV URL from the HTML
  - Downloading the CSV
  - Concatenating everything into one big CSV

Output: 100_Data/columbiadaily_raw.csv
"""

import re
import sys
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

BASE_URL = "https://www.fpc.org"
RESULT_URL = f"{BASE_URL}/adults/R_dailyadultcountsgraph_resultsV6.php"

# Headers modeled off your real browser + HAR capture
COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/18.1.1 Safari/605.1.15"
    ),
    "Referer": f"{BASE_URL}/adults/Q_dailyadultcountsgraph2.php",
    "Origin": BASE_URL,
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Dam codes -> nice names (your list)
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
    "WFA": "Willamette Falls Dam",
}

# Species codes -> nice names (your list)
SPECIES_CODES = {
    "CHAD": "Chinook Adult",
    "COAD": "Coho Adult",
    "ST": "Steelhead",
    "WST": "Unclipped Steelhead",
    "SO": "Sockeye",
}

# 14 dams × 5 species = 70 combos (all cross-product)
DAM_SPECIES_PAIRS = [
    (dam_code, species_code)
    for dam_code in DAM_CODES.keys()
    for species_code in SPECIES_CODES.keys()
]

# Regex to find CSV URL inside the HTML
CSV_REGEX = re.compile(r'/DataReqs/web/apps/adultsalmon/[^"]+\.csv')


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def fetch_results_html(session: requests.Session, dam_code: str, species_code: str) -> str | None:
    """
    POST to the results page with dam/species and return HTML text.
    Returns None on failure.
    """
    data = {"dam": dam_code, "species": species_code}

    try:
        resp = session.post(RESULT_URL, headers=COMMON_HEADERS, data=data, timeout=30)
    except Exception as e:
        print(f"      ❌ POST error: {e}")
        return None

    if resp.status_code != 200:
        print(f"      ❌ POST status {resp.status_code}")
        return None

    return resp.text


def extract_csv_url(html: str) -> str | None:
    """
    From the HTML of the results page, extract the CSV path using regex,
    and return the full URL.
    """
    m = CSV_REGEX.search(html)
    if not m:
        return None

    path = m.group(0)
    # Ensure we have a leading slash
    if not path.startswith("/"):
        path = "/" + path

    return BASE_URL + path


def download_csv(session: requests.Session, csv_url: str) -> str | None:
    """
    Download CSV text from the given URL.
    Returns CSV string or None on failure.
    """
    try:
        resp = session.get(csv_url, headers={"User-Agent": COMMON_HEADERS["User-Agent"]}, timeout=30)
    except Exception as e:
        print(f"      ❌ CSV GET error: {e}")
        return None

    if resp.status_code != 200:
        print(f"      ❌ CSV GET status {resp.status_code}")
        return None

    return resp.text


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> int:
    print("🐟 Step 1: Pulling Columbia/Snake River daily counts via pure HTTP...\n")

    # Project layout: this file is /10_columbiadaily/1_datapull.py
    # Data folder is sibling: /100_Data
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1]
    data_dir = project_root / "100_Data"
    data_dir.mkdir(parents=True, exist_ok=True)

    out_csv_path = data_dir / "columbiadaily_raw.csv"

    all_frames: list[pd.DataFrame] = []
    successful_pairs: list[tuple[str, str]] = []

    with requests.Session() as session:
        for dam_code, species_code in DAM_SPECIES_PAIRS:
            dam_name = DAM_CODES[dam_code]
            species_name = SPECIES_CODES[species_code]

            print(f"🌐 Fetching: {dam_name} — {species_name} ({dam_code}/{species_code})")

            # 1) POST to results page
            html = fetch_results_html(session, dam_code, species_code)
            if not html:
                print("   ⚠️ No HTML returned, skipping.\n")
                continue

            # 2) Extract CSV URL from HTML
            csv_url = extract_csv_url(html)
            if not csv_url:
                print("   ⚠️ No CSV link found in HTML, skipping.\n")
                continue

            print(f"   📎 CSV URL: {csv_url}")

            # 3) Download CSV
            csv_text = download_csv(session, csv_url)
            if not csv_text:
                print("   ⚠️ Could not download CSV, skipping.\n")
                continue

            # 4) Parse CSV into DataFrame (keep raw structure)
            try:
                df = pd.read_csv(pd.compat.StringIO(csv_text))
            except AttributeError:
                # pandas >= 2.0 removed compat.StringIO; fall back to io.StringIO
                import io
                df = pd.read_csv(io.StringIO(csv_text))
            except Exception as e:
                print(f"   ❌ pandas.read_csv error: {e}")
                # Optionally, dump CSV to file for debugging
                debug_path = data_dir / f"debug_{dam_code}_{species_code}.csv"
                try:
                    debug_path.write_text(csv_text, encoding="utf-8")
                    print(f"   📝 Wrote raw CSV to {debug_path} for inspection.\n")
                except Exception:
                    print("   ⚠️ Also failed to write debug CSV.\n")
                continue

            # 5) Add metadata columns so you always know which dam/species
            df["dam_code"] = dam_code
            df["dam_name"] = dam_name
            df["species_code"] = species_code
            df["species_name"] = species_name

            row_count = len(df)
            print(f"   ✅ Parsed {row_count} rows.\n")

            if row_count > 0:
                all_frames.append(df)
                successful_pairs.append((dam_code, species_code))

    if not all_frames:
        print("❌ No data collected for any dam/species. Nothing to write.")
        return 1

    # Concatenate everything into one big CSV
    big_df = pd.concat(all_frames, ignore_index=True)

    big_df.to_csv(out_csv_path, index=False)
    print("----------------------------------------------------------------")
    print(f"✅ Wrote {len(big_df)} total rows from {len(successful_pairs)} "
          f"dam/species combos to:\n   {out_csv_path}")
    print("----------------------------------------------------------------")

    return 0


if __name__ == "__main__":
    sys.exit(main())