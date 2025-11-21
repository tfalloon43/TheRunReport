# 10_columbiadata.py
# --------------------------------------------------------------------
# Fetch daily adult counts for all dams/species from the Fish Passage
# Center (FPC) daily adult counts endpoint and save normalized outputs.
#
# Endpoint flow:
#   POST https://www.fpc.org/adults/R_dailyadultcountsgraph_resultsV6.php
#       data: dam=<code>, species=<code>
#   Response HTML embeds a CSV path like:
#       url:'/DataReqs/web/apps/adultsalmon/XXXX.csv'
#   The CSV contains:
#       Dates, Dam, Species,
#       Daily_Count_Current_Year, Daily_Count_Last_Year,
#       Ten_Year_Average_Daily_Count
#
# Outputs (in 100_Data/):
#   - columbia_daily_counts_raw.csv  : raw concatenated CSV rows + metadata
#   - columbia_daily_counts_long.csv : long format ready for plotting
# --------------------------------------------------------------------

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

# Form codes exposed on the FPC page (Q_dailyadultcountsgraph2.php)
DAM_CODES: Dict[str, str] = {
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

SPECIES_CODES: Dict[str, str] = {
    "CHAD": "Chinook Adult",
    "CHJC": "Chinook Jack",
    "CHSPAD": "Spring Chinook Adult",
    "CHSPJC": "Spring Chinook Jack",
    "CHSUAD": "Summer Chinook Adult",
    "CHSUJC": "Summer Chinook Jack",
    "CHFAAD": "Fall Chinook Adult",
    "CHFAJC": "Fall Chinook Jack",
    "COAD": "Coho Adult",
    "COJC": "Coho Jack",
    "ST": "Steelhead",
    "WST": "Unclipped Steelhead",
    "SO": "Sockeye",
    "LAMPREY": "Lamprey",
    "SH": "Shad",
}

ENDPOINT = "https://www.fpc.org/adults/R_dailyadultcountsgraph_resultsV6.php"
CSV_BASE = "https://www.fpc.org"
CSV_REGEX = re.compile(r"url:'(?P<path>/DataReqs/web/apps/adultsalmon/[^']+\\.csv)'")
HEADERS = {"User-Agent": "Mozilla/5.0"}  # Some endpoints block non-browser UA
ALT_CSV_REGEX = re.compile(
    r"(/DataReqs/web/apps/adultsalmon/[A-Za-z0-9_-]+\\.csv)", re.IGNORECASE
)

project_root = Path(__file__).resolve().parent
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)


def extract_csv_url(html: str) -> str:
    """Pull the CSV path from the HTML response (robust against minor markup changes)."""
    match = CSV_REGEX.search(html)
    if match:
        return CSV_BASE + match.group("path")

    alt = ALT_CSV_REGEX.search(html)
    if alt:
        return CSV_BASE + alt.group(1)

    raise ValueError("CSV URL not found in response HTML")


def fetch_csv(dam_code: str, species_code: str) -> pd.DataFrame:
    """Request CSV for one dam/species pair and return as DataFrame."""
    session = requests.Session()
    resp = session.post(
        ENDPOINT,
        data={"dam": dam_code, "species": species_code},
        headers={
            **HEADERS,
            "Referer": "https://www.fpc.org/adults/Q_dailyadultcountsgraph2.php",
        },
        timeout=45,
    )
    resp.raise_for_status()
    csv_url = extract_csv_url(resp.text)
    df = pd.read_csv(csv_url)

    # Attach metadata for later joins/pivots
    df["dam_code"] = dam_code
    df["dam_name"] = DAM_CODES[dam_code]
    df["species_code"] = species_code
    df["species_name"] = SPECIES_CODES[species_code]
    df["downloaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
    return df


def normalize_long(df: pd.DataFrame, current_year: int) -> pd.DataFrame:
    """Convert wide CSV schema to long format for plotting."""
    records: List[dict] = []

    for _, row in df.iterrows():
        mm_dd = str(row.get("Dates", "")).strip()
        try:
            month, day = map(int, mm_dd.split("/"))
        except Exception:
            continue  # skip malformed date rows

        meta = {
            "dam_code": row["dam_code"],
            "dam_name": row["dam_name"],
            "species_code": row["species_code"],
            "species_name": row["species_name"],
            "mm_dd": mm_dd,
        }

        # Current year
        if pd.notna(row.get("Daily_Count_Current_Year")):
            records.append(
                {
                    **meta,
                    "series": "current_year",
                    "date": datetime(current_year, month, day).date().isoformat(),
                    "value": float(row["Daily_Count_Current_Year"]),
                }
            )

        # Last year
        if pd.notna(row.get("Daily_Count_Last_Year")):
            records.append(
                {
                    **meta,
                    "series": "last_year",
                    "date": datetime(current_year - 1, month, day).date().isoformat(),
                    "value": float(row["Daily_Count_Last_Year"]),
                }
            )

        # Ten-year average (align to current-year calendar)
        if pd.notna(row.get("Ten_Year_Average_Daily_Count")):
            records.append(
                {
                    **meta,
                    "series": "ten_year_avg",
                    "date": datetime(current_year, month, day).date().isoformat(),
                    "value": float(row["Ten_Year_Average_Daily_Count"]),
                }
            )

    long_df = pd.DataFrame.from_records(records)
    return long_df


def main() -> None:
    print("🚚 Fetching Columbia Basin daily adult counts from FPC...")
    all_frames: List[pd.DataFrame] = []
    failures: List[str] = []

    for dam_code in DAM_CODES:
        for species_code in SPECIES_CODES:
            tag = f"{dam_code}:{species_code}"
            try:
                df = fetch_csv(dam_code, species_code)
                all_frames.append(df)
                print(f"  ✅ {tag}")
            except Exception as exc:  # pragma: no cover - network handling
                failures.append(f"{tag} ({exc})")
                print(f"  ⚠️  {tag} failed: {exc}")

    if not all_frames:
        raise RuntimeError("No FPC CSVs were fetched. Aborting.")

    raw = pd.concat(all_frames, ignore_index=True)
    raw_path = data_dir / "columbia_daily_counts_raw.csv"
    raw.to_csv(raw_path, index=False)

    long_df = normalize_long(raw, current_year=datetime.now().year)
    long_path = data_dir / "columbia_daily_counts_long.csv"
    long_df.to_csv(long_path, index=False)

    print("\n📁 Saved outputs:")
    print(f"   - Raw : {raw_path} ({len(raw):,} rows)")
    print(f"   - Long: {long_path} ({len(long_df):,} rows)")

    if failures:
        print("\n⚠️ Some requests failed:")
        for f in failures:
            print(f"   - {f}")

    print("\n🎯 Done.")


if __name__ == "__main__":
    main()
