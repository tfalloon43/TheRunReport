# 16_NOAAflow.py
# ------------------------------------------------------------
# Fetch NOAA flow + stage data for all NOAA sites in flows.csv
#
# Uses the official NWPS API:
#   https://api.water.noaa.gov/nwps/v1/gauges/{identifier}/stageflow
#
# Input:
#   • 100_Data/flows.csv
#
# Output:
#   • 100_Data/NOAA_flows.csv
#
# Columns:
#   - timestamp   (ISO datetime from NOAA, validTime)
#   - window      ('7d', '30d', '1y')
#   - site_id     (NOAA gauge ID, e.g. ECHW1)
#   - river       (from flows.csv 'river')
#   - site_name   (from flows.csv 'Site n')
#   - stage_ft    (observed.primary, usually ft)
#   - flow_cfs    (observed.secondary, converted to cfs if units = kcfs)
# ------------------------------------------------------------

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone

print("🌊 Step 16: Fetching NOAA flow + stage data (NWPS API)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

flows_path = data_dir / "flows.csv"
output_path = data_dir / "NOAA_flows.csv"

if not flows_path.exists():
    raise FileNotFoundError(f"❌ flows.csv not found at: {flows_path}")

print(f"📂 Reading site definitions from: {flows_path}")
flows_df = pd.read_csv(flows_path)

# ------------------------------------------------------------
# Time windows (UTC, timezone-aware)
# ------------------------------------------------------------
now_utc = datetime.now(timezone.utc)

WINDOWS = {
    "7d":  now_utc - timedelta(days=7),
    "30d": now_utc - timedelta(days=30),
    "1y":  now_utc - timedelta(days=365),
}

# ------------------------------------------------------------
# Helpers to find NOAA sites
# ------------------------------------------------------------
def is_noaa_code(val) -> bool:
    """
    NOAA IDs are non-numeric strings like ECHW1, WASW1, etc.
    We treat purely numeric IDs as *not* NOAA (those are USGS).
    """
    if pd.isna(val):
        return False
    s = str(val).strip()
    return not s.isdigit()


def iter_noaa_sites(df: pd.DataFrame):
    """
    Yields dicts:
        { "site_id", "river", "site_name" }
    for every NOAA site in flows.csv:
      - flow_presence contains 'NOAA' or 'BOTH' (case-insensitive)
      - Gage #n is a non-numeric code (ECHW1, SDQW1, etc.)
    """
    max_sites = 14

    for _, row in df.iterrows():
        fp = str(row.get("flow_presence", "")).upper()
        if "NOAA" not in fp and fp != "BOTH":
            continue

        river = row.get("river", "")

        for i in range(1, max_sites + 1):
            site_col = f"Site {i}"
            gage_col = f"Gage #{i}"

            # If we run out of site/gage columns, stop scanning this row
            if site_col not in df.columns or gage_col not in df.columns:
                break

            site_name = row.get(site_col, "")
            gage_id = row.get(gage_col, None)

            if pd.isna(gage_id):
                continue

            if is_noaa_code(gage_id):
                site_id = str(gage_id).strip().upper()
                yield {
                    "site_id": site_id,
                    "river": river,
                    "site_name": site_name,
                }


# ------------------------------------------------------------
# NOAA NWPS API
# ------------------------------------------------------------
BASE_URL = "https://api.water.noaa.gov/nwps/v1/gauges"

def fetch_noaa_stageflow(site_id: str) -> dict | None:
    """
    Calls:
        GET https://api.water.noaa.gov/nwps/v1/gauges/{site_id}/stageflow
    Returns parsed JSON or None on error.
    """
    url = f"{BASE_URL}/{site_id}/stageflow"
    try:
        r = requests.get(url, timeout=20)
    except Exception as e:
        print(f"   ❌ {site_id}: request error → {e}")
        return None

    if r.status_code != 200:
        print(f"   ❌ {site_id}: HTTP {r.status_code} for {url}")
        return None

    try:
        return r.json()
    except ValueError as e:
        print(f"   ❌ {site_id}: JSON decode error → {e}")
        return None


def parse_noaa_stageflow(
    json_data: dict,
    *,
    site_id: str,
    river: str,
    site_name: str,
) -> pd.DataFrame:
    """
    Parse NWPS stageflow JSON into a tidy dataframe.

    Expected structure (based on NOAA examples + community use):

    {
      "observed": {
        "primaryName": "Stage",
        "primaryUnits": "ft",
        "secondaryName": "Flow",
        "secondaryUnits": "kcfs" or "cfs",
        "data": [
          {
            "validTime": "2025-05-30T21:45:00Z",
            "generatedTime": "...",
            "primary": 1.43,
            "secondary": 0.11
          },
          ...
        ]
      },
      "forecast": { ... }   # may also be present, but we ignore for now
    }
    """
    if not json_data or "observed" not in json_data:
        return pd.DataFrame()

    observed = json_data["observed"]
    data_list = observed.get("data", [])

    if not data_list:
        return pd.DataFrame()

    primary_units = observed.get("primaryUnits", "").lower()
    secondary_units = observed.get("secondaryUnits", "").lower()

    rows = []

    for item in data_list:
        ts = item.get("validTime")
        if ts is None:
            continue

        primary_val = item.get("primary")
        secondary_val = item.get("secondary")

        # Stage
        try:
            stage_ft = float(primary_val) if primary_val is not None else None
        except (TypeError, ValueError):
            stage_ft = None

        # Flow, convert to cfs if needed
        flow_cfs = None
        if secondary_val is not None:
            try:
                flow_val = float(secondary_val)
            except (TypeError, ValueError):
                flow_val = None

            if flow_val is not None:
                if secondary_units == "kcfs":
                    flow_cfs = flow_val * 1000.0
                else:
                    # assume already in cfs (or at least, direct numeric)
                    flow_cfs = flow_val

        rows.append(
            {
                "timestamp": ts,
                "site_id": site_id,
                "river": river,
                "site_name": site_name,
                "stage_ft": stage_ft,
                "flow_cfs": flow_cfs,
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_dt"])

    return df.sort_values("timestamp_dt")


# ------------------------------------------------------------
# Main process
# ------------------------------------------------------------
site_records = list(iter_noaa_sites(flows_df))

if not site_records:
    print("📭 No NOAA sites found in flows.csv (flow_presence != NOAA/BOTH).")
    raise SystemExit(0)

# De-duplicate by (site_id, river, site_name)
unique_map = {
    (rec["site_id"], rec["river"], rec["site_name"]): rec
    for rec in site_records
}
site_records = list(unique_map.values())

print(f"🔎 Found {len(site_records)} unique NOAA gauges in flows.csv")

all_frames: list[pd.DataFrame] = []

for rec in site_records:
    site_id = rec["site_id"]
    river = rec["river"]
    site_name = rec["site_name"]

    print(f"\n📡 NOAA Site {site_id} | River: {river} | Name: {site_name}")

    json_data = fetch_noaa_stageflow(site_id)
    if not json_data:
        print("   ↳ No data (request or JSON error).")
        continue

    df_raw = parse_noaa_stageflow(
        json_data,
        site_id=site_id,
        river=river,
        site_name=site_name,
    )

    if df_raw.empty:
        print("   ↳ No observed data parsed.")
        continue

    # Apply time windows
    for label, start_dt in WINDOWS.items():
        start_ts = pd.Timestamp(start_dt)
        df_window = df_raw[df_raw["timestamp_dt"] >= start_ts].copy()

        if df_window.empty:
            print(f"   ↳ No {label} data in window.")
            continue

        df_window["window"] = label
        all_frames.append(df_window)
        print(f"   ↳ {label}: {len(df_window)} rows")

# ------------------------------------------------------------
# Combine & save
# ------------------------------------------------------------
if not all_frames:
    print("📭 No NOAA data gathered for any gauge/window — NOAA_flows.csv not created.")
    raise SystemExit(0)

df_all = pd.concat(all_frames, ignore_index=True)
df_all = df_all.sort_values("timestamp_dt")

# Keep clean output columns
df_all = df_all[
    [
        "timestamp",
        "window",
        "site_id",
        "river",
        "site_name",
        "stage_ft",
        "flow_cfs",
        "timestamp_dt",
    ]
]

df_all.to_csv(output_path, index=False)

print("\n📋 SUMMARY")
print(f"   • Total rows: {len(df_all)}")
print(f"   • Output written to: {output_path}")

print("\n🎯 Step 16 complete: NOAA_flows.csv created using NWPS stageflow API.")