# 15_USGSflow.py
# ------------------------------------------------------------
# Fetch USGS flow + stage data for all USGS sites in the Flows table
#
# Input:
#   • local.db table: Flows
#
#     Expected columns:
#       - river
#       - flow_presence
#       - river_name
#       - Site 1, Gage #1
#       - Site 2, Gage #2
#       - ...
#       - Site 14, Gage #14
#
#   Notes:
#       • USGS gage IDs are expected to be numeric in the Gage #n columns.
#       • Non-numeric IDs (e.g., NOAA codes like 'ECHW1') are skipped.
#
# Output table:
#   • local.db table: USGS_flows
#
# Columns:
#   - timestamp (ISO datetime from USGS)
#   - window    ('7d', '30d', '1y')
#   - site_id   (USGS gage ID as string)
#   - river     (from Flows table 'river')
#   - site_name (from the corresponding 'Site n' column)
#   - flow_cfs  (USGS parameter 00060)
#   - stage_ft  (USGS parameter 00065)
#
# Time windows:
#   - Last 7 days
#   - Last 30 days
#   - Last 1 year
#
# ------------------------------------------------------------

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
import os
import sqlite3

print("🌊 Step 15 (Flows): Fetching USGS flow + stage data for all USGS sites in Flows...")

# ------------------------------------------------------------
# DB PATH / TABLES
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
TABLE_FLOWS = "Flows"
TABLE_USGS_FLOWS = "USGS_flows"

print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# Load Flows table
# ------------------------------------------------------------
with sqlite3.connect(db_path) as conn:
    try:
        flows_df = pd.read_sql_query(f"SELECT * FROM [{TABLE_FLOWS}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_FLOWS}] in local.db: {e}")

# Basic sanity check for expected columns
required_columns = {"river"}
missing = required_columns - set(flows_df.columns)
if missing:
    raise ValueError(f"❌ [{TABLE_FLOWS}] is missing required columns: {missing}")

# ------------------------------------------------------------
# USGS NWIS JSON API endpoint & parameters
# ------------------------------------------------------------
USGS_API = "https://waterservices.usgs.gov/nwis/iv/"

# Parameters:
#   00060 = discharge (cfs)
#   00065 = gage height (ft)
PARAMS = ["00060", "00065"]

# ------------------------------------------------------------
# Time ranges
# ------------------------------------------------------------
now = datetime.utcnow()

def fmt(dt: datetime) -> str:
    """Format datetime as ISO string expected by USGS."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

RANGES = {
    "7d":  fmt(now - timedelta(days=7)),
    "30d": fmt(now - timedelta(days=30)),
    "1y":  fmt(now - timedelta(days=365)),
}

include_1y = os.getenv("FLOWS_INCLUDE_1Y", "1").strip().lower() in {"1", "true", "yes", "y"}
if not include_1y:
    RANGES = {k: v for k, v in RANGES.items() if k != "1y"}
    print("🪓 1y window disabled (FLOWS_INCLUDE_1Y=0).")

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------
def normalize_gage_id(raw) -> str | None:
    """
    Normalize a gage ID from Flows to a USGS site_id string.

    - Accepts numeric values (float/int), converts to int then str.
    - Accepts digit-only strings.
    - Returns None for anything non-numeric (e.g., NOAA IDs like 'ECHW1').
    """
    if pd.isna(raw):
        return None

    # If it's already a number (e.g. 12190400.0)
    if isinstance(raw, (int, float)):
        # Ignore NaN floats
        if isinstance(raw, float) and pd.isna(raw):
            return None
        return str(int(raw))

    s = str(raw).strip()

    # Handle stuff like "12190400.0"
    if s.endswith(".0") and s.replace(".", "", 1).isdigit():
        try:
            return str(int(float(s)))
        except ValueError:
            return None

    # Pure digits (e.g., "12190400")
    if s.isdigit():
        return s

    # Anything else (NOAA codes etc.) → not a USGS ID
    return None


def iter_usgs_sites(flows: pd.DataFrame):
    """
    Yield (site_id, river, site_name) for every valid USGS site in Flows.

    We scan columns:
        Site 1, Gage #1
        Site 2, Gage #2
        ...
        Site 14, Gage #14
    """
    max_sites = 14
    for _, row in flows.iterrows():
        river = row.get("river", None)

        for i in range(1, max_sites + 1):
            site_col = f"Site {i}"
            gage_col = f"Gage #{i}"

            if site_col not in flows.columns or gage_col not in flows.columns:
                # If later columns don't exist, stop checking
                break

            site_name = row.get(site_col, None)
            raw_gage = row.get(gage_col, None)

            # Skip empty rows
            if (pd.isna(site_name) or str(site_name).strip() == "") and pd.isna(raw_gage):
                continue

            site_id = normalize_gage_id(raw_gage)

            # Skip non-USGS (e.g., NOAA codes)
            if site_id is None:
                continue

            yield {
                "site_id": site_id,
                "river": river,
                "site_name": site_name,
            }


def fetch_usgs(site_id: str, start_dt: str) -> dict | None:
    """Fetch USGS JSON for a given site and start datetime."""
    params = {
        "format": "json",
        "sites": site_id,
        "startDT": start_dt,
        "parameterCd": ",".join(PARAMS),
        "siteStatus": "all",
    }

    try:
        r = requests.get(USGS_API, params=params, timeout=20)
    except Exception as e:
        print(f"❌ Request error for site {site_id}: {e}")
        return None

    if r.status_code != 200:
        print(f"❌ USGS request failed for site {site_id} → HTTP {r.status_code}")
        return None

    try:
        return r.json()
    except ValueError as e:
        print(f"❌ JSON decode error for site {site_id}: {e}")
        return None


def parse_usgs(json_data: dict, *, window_label: str, site_id: str, river: str, site_name: str) -> pd.DataFrame:
    """
    Convert a USGS JSON response into a tidy dataframe for one site + window.

    Columns:
        - timestamp
        - window
        - site_id
        - river
        - site_name
        - parameter (flow_cfs / stage_ft)
        - value
    """
    if (
        not json_data
        or "value" not in json_data
        or "timeSeries" not in json_data["value"]
        or not json_data["value"]["timeSeries"]
    ):
        return pd.DataFrame()

    rows = []

    for series in json_data["value"]["timeSeries"]:
        variable_code = series["variable"]["variableCode"][0]["value"]
        if variable_code == "00060":
            parameter_name = "flow_cfs"
        elif variable_code == "00065":
            parameter_name = "stage_ft"
        else:
            # Ignore any parameters we didn't ask for
            continue

        values_list = series.get("values", [])
        if not values_list:
            continue

        points = values_list[0].get("value", [])
        for p in points:
            ts = p.get("dateTime")
            val = p.get("value")

            if ts is None or val is None:
                continue

            try:
                val_float = float(val)
            except ValueError:
                # Skip malformed values
                continue

            rows.append({
                "timestamp": ts,
                "window": window_label,
                "site_id": site_id,
                "river": river,
                "site_name": site_name,
                "parameter": parameter_name,
                "value": val_float,
            })

    return pd.DataFrame(rows)


# ------------------------------------------------------------
# Main execution
# ------------------------------------------------------------
# Collect all unique USGS site definitions
site_records = list(iter_usgs_sites(flows_df))

if not site_records:
    print("📭 No valid USGS sites found in Flows — nothing to fetch.")
    raise SystemExit(0)

print(f"✅ Found {len(site_records)} USGS site entries in Flows")

# Optional: de-duplicate by (site_id, river, site_name) if needed
# This keeps the code straightforward while avoiding redundant work.
unique_site_records = {
    (rec["site_id"], rec["river"], rec["site_name"]): rec
    for rec in site_records
}
site_records = list(unique_site_records.values())

print(f"🔁 After de-duplication: {len(site_records)} unique site records")

all_frames: list[pd.DataFrame] = []

# Simple cache so we don't hit the API multiple times per site/window
response_cache: dict[tuple[str, str], dict | None] = {}

for rec in site_records:
    site_id = rec["site_id"]
    river = rec["river"]
    site_name = rec["site_name"]

    print(f"\n📡 Site {site_id} | River: {river} | Site name: {site_name}")

    for window_label, startDT in RANGES.items():
        cache_key = (site_id, window_label)

        if cache_key in response_cache:
            json_result = response_cache[cache_key]
        else:
            print(f"   • Fetching {window_label} data starting {startDT}...")
            json_result = fetch_usgs(site_id, startDT)
            response_cache[cache_key] = json_result

        df_window = parse_usgs(
            json_result,
            window_label=window_label,
            site_id=site_id,
            river=river,
            site_name=site_name,
        )

        if df_window.empty:
            print(f"     ↳ No data returned for this window.")
        else:
            print(f"     ↳ Retrieved {len(df_window)} rows.")
            all_frames.append(df_window)

# ------------------------------------------------------------
# Combine → pivot → save
# ------------------------------------------------------------
if not all_frames:
    print("📭 No data retrieved for any site/window — USGS_flows table not created.")
    raise SystemExit(0)

full_long_df = pd.concat(all_frames, ignore_index=True)

# Pivot: one row per timestamp + site + window, with both stage + flow if available
wide_df = full_long_df.pivot_table(
    index=["timestamp", "window", "site_id", "river", "site_name"],
    columns="parameter",
    values="value",
).reset_index()

# Flatten column names (pivot creates a MultiIndex for columns)
wide_df.columns.name = None
wide_df.insert(0, "id", range(1, len(wide_df) + 1))

with sqlite3.connect(db_path) as conn:
    wide_df.to_sql(TABLE_USGS_FLOWS, conn, if_exists="replace", index=False)

print("\n📋 SUMMARY")
print(f"   • Total rows (long format): {len(full_long_df)}")
print(f"   • Total rows (wide format): {len(wide_df)}")
print(f"   • Output table: [{TABLE_USGS_FLOWS}]")

print("\n🎯 Step 15 complete: USGS_flows table created.")
