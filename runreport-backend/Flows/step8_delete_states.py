# 8_delete_states.py
# ------------------------------------------------------------
# Step 8 (Flows): Filter NOAA catalog down to Washington-only
# stations and overwrite Flows_NOAA_completelist in local.db.
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🧹 Step 8 (Flows): Filtering NOAA catalog to Washington-only stations…")

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

if "State" not in df.columns:
    raise ValueError("❌ Column 'State' not found in NOAA catalog table.")

df_filtered = df[df["State"].astype(str).str.strip().str.upper() == "WA"].copy()
print(f"✅ Kept {len(df_filtered):,} Washington rows")

with sqlite3.connect(db_path) as conn:
    df_filtered.to_sql(TABLE_NOAA_CATALOG, conn, if_exists="replace", index=False)

print(f"🔄 Updated [{TABLE_NOAA_CATALOG}] in place")
print("✅ Step 8 complete.")

