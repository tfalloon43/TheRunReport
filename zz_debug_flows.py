# zz_debug_flows.py
# ------------------------------------------------------------
# DEBUG UTILITY (Flows)
#
# Exports:
#   1) The Flows table from runreport-backend/0_db/local.db
#   2) The legacy CSV pipeline file 100_Data/flows.csv
#
# into:
#   ~/Desktop/zz_tester
#
# Outputs:
#   - DB_flows.csv
#   - CSV_flows.csv
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🧪 zz_debug_flows.py — CSV vs DB snapshot tool (Flows)")

# ============================================================
# Paths
# ============================================================

PROJECT_ROOT = Path("/Users/thomasfalloon/Desktop/TheRunReport")
DB_PATH = PROJECT_ROOT / "runreport-backend" / "0_db" / "local.db"
CSV_PATH = PROJECT_ROOT / "100_Data" / "flows.csv"

OUTPUT_DIR = Path("/Users/thomasfalloon/Desktop/zz_tester")
OUTPUT_DIR.mkdir(exist_ok=True)

DB_OUT = OUTPUT_DIR / "DB_flows.csv"
CSV_OUT = OUTPUT_DIR / "CSV_flows.csv"

# ============================================================
# 1️⃣ EXPORT DB TABLE TO CSV
# ============================================================

if not DB_PATH.exists():
    raise FileNotFoundError(f"❌ DB not found: {DB_PATH}")

with sqlite3.connect(DB_PATH) as conn:
    df_db = pd.read_sql_query("SELECT * FROM Flows;", conn)

df_db.to_csv(DB_OUT, index=False)
print(f"📄 Exported DB Flows table → {DB_OUT} (rows: {len(df_db):,})")

# ============================================================
# 2️⃣ COPY CSV FROM LEGACY PIPELINE
# ============================================================

if not CSV_PATH.exists():
    raise FileNotFoundError(f"❌ CSV not found: {CSV_PATH}")

df_csv = pd.read_csv(CSV_PATH)
df_csv.to_csv(CSV_OUT, index=False)

print(f"📄 Copied legacy flows.csv → {CSV_OUT} (rows: {len(df_csv):,})")

print("\n✅ Debug snapshot complete")

