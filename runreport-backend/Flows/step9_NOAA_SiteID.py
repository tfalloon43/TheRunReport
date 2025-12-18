# 9_NOAA_siteID.py
# ------------------------------------------------------------
# Step 9 (Flows): Extract NOAA "Site ID" from column "ID" in the
# NOAA catalog table and update the table in place.
#
# Logic:
#   Site ID = substring of ID from start → first "1" (inclusive)
#   Example: "SHAW1SEWBAKER..." → "SHAW1"
#
# Input/Output table:
#   - Flows_NOAA_completelist
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🔎 Step 9 (Flows): Extracting NOAA Site ID from ID column...")

TABLE_NOAA_CATALOG = "Flows_NOAA_completelist"

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

with sqlite3.connect(db_path) as conn:
    try:
        df = pd.read_sql_query(f"SELECT * FROM [{TABLE_NOAA_CATALOG}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_NOAA_CATALOG}] in local.db: {e}")

print(f"📂 Loaded {len(df):,} rows from [{TABLE_NOAA_CATALOG}]")

if "ID" not in df.columns:
    raise ValueError("❌ 'ID' column not found in NOAA catalog table.")


def extract_site_id(full_id: str) -> str:
    if not isinstance(full_id, str):
        return ""
    idx = full_id.find("1")
    if idx == -1:
        return ""
    return full_id[: idx + 1]


df["Site ID"] = df["ID"].astype(str).apply(extract_site_id)
print("🔧 Extracted Site ID column.")

with sqlite3.connect(db_path) as conn:
    df.to_sql(TABLE_NOAA_CATALOG, conn, if_exists="replace", index=False)

print(f"🔄 Updated [{TABLE_NOAA_CATALOG}] in place")
print("✅ Step 9 complete.")

