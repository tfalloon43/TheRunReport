# 3_rivername.py
# ------------------------------------------------------------
# Step 3 (Flows): Extract canonical river/creek name from USGS
# site_name strings and store results in local.db.
#
# Input table:
#   - Flows_USGSsites
#
# Output tables:
#   - Flows_USGSsites_rivername (adds river_name)
#   - Flows (adds/updates river_name via substring matching)
# ------------------------------------------------------------

import re
import sqlite3
from pathlib import Path

import pandas as pd

print("🌊 Step 3 (Flows): Extracting standardized river_name from USGS sites…")

TABLE_FLOWS = "Flows"
TABLE_USGS_SITES = "Flows_USGSsites"
TABLE_USGS_RIVERNAMES = "Flows_USGSsites_rivername"

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

with sqlite3.connect(db_path) as conn:
    try:
        df = pd.read_sql_query(f"SELECT * FROM [{TABLE_USGS_SITES}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_USGS_SITES}] in local.db: {e}")

print(f"📘 Loaded {len(df):,} USGS sites from [{TABLE_USGS_SITES}]")


def extract_river_name(name: str) -> str:
    if not isinstance(name, str):
        return ""

    text = name.lower()

    patterns = [
        r"(.+?river)\b",
        r"(.+?creek)\b",
        r"(.+?fork.+?river)\b",
        r"(.+?fork.+?creek)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            extracted = match.group(1)
            extracted = extracted.replace(",", "").strip()
            return extracted.title()

    base = name.split(",")[0].strip()
    return base.title()


if "site_name" not in df.columns:
    raise ValueError(f"❌ [{TABLE_USGS_SITES}] missing required column: site_name")

df["river_name"] = df["site_name"].apply(extract_river_name)
print("🔍 Extracted river_name for all stations.")

with sqlite3.connect(db_path) as conn:
    df.to_sql(TABLE_USGS_RIVERNAMES, conn, if_exists="replace", index=False)

print(f"💾 Saved → table [{TABLE_USGS_RIVERNAMES}]")

with sqlite3.connect(db_path) as conn:
    try:
        flows = pd.read_sql_query(f"SELECT * FROM [{TABLE_FLOWS}];", conn)
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing source table [{TABLE_FLOWS}] in local.db: {e}")

print("📘 Updating Flows table with river_name matches…")


def match_river(river: str) -> str:
    river_lower = str(river).lower().strip()
    if not river_lower:
        return ""

    matches = df[df["river_name"].astype(str).str.lower().str.contains(river_lower, na=False)]
    if matches.empty:
        return ""

    return ", ".join(sorted(pd.Series(matches["river_name"].tolist()).dropna().unique().tolist()))


if "river" not in flows.columns:
    raise ValueError(f"❌ [{TABLE_FLOWS}] missing required column: river")

flows["river_name"] = flows["river"].astype(str).apply(match_river)

with sqlite3.connect(db_path) as conn:
    flows.to_sql(TABLE_FLOWS, conn, if_exists="replace", index=False)

print(f"🔄 Updated [{TABLE_FLOWS}] with river_name column.")
print("✅ Step 3 complete — river names extracted and flows updated.")

