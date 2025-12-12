# zz_debug.py
# ------------------------------------------------------------
# DEBUG UTILITY
#
# Copies:
#   1) A chosen CSV from the legacy CSV pipeline
#   2) A chosen table from local.db
#
# into:
#   ~/Desktop/zz_tester
#
# You manually change:
#   - CSV_NAME
#   - DB_TABLE_NAME
#
# No smart logic. No inference. Explicit only.
# ------------------------------------------------------------

import shutil
import sqlite3
import pandas as pd
from pathlib import Path

print("🧪 zz_debug.py — CSV vs DB snapshot tool")

# ============================================================
# 🔧🔧🔧 MANUAL EDIT SECTION (CHANGE THESE)
# ============================================================

# --- CSV produced by 0_master_pipeline ---
CSV_NAME = "csv_reduce.csv"     # 👈 CHANGE THIS AS NEEDED

# --- DB table to export ---
DB_TABLE_NAME = "Escapement_PlotPipeline"   # 👈 CHANGE THIS AS NEEDED

# ============================================================
# Paths
# ============================================================

PROJECT_ROOT = Path("/Users/thomasfalloon/Desktop/TheRunReport")

CSV_SOURCE_DIR = PROJECT_ROOT / "100_Data"
DB_PATH        = PROJECT_ROOT / "runreport-backend" / "0_db" / "local.db"

OUTPUT_DIR = Path("/Users/thomasfalloon/Desktop/zz_tester")
OUTPUT_DIR.mkdir(exist_ok=True)

SORT_COLUMNS = ["pdf_name", "basin", "species", "Stock", "date_iso"]
# Columns to remove from both outputs (set to [] to keep all). Example: ["species"]
COLUMNS_TO_DROP = [
#                   "species", 
#                   "stock_presence", 
#                   "stock_presence_lower",
#                   "Hatchery_Name",
#                   "Family",
#                   "facility",
                   "",
                   "",
                   "",
                   "",]

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def sorted_or_warn(df: pd.DataFrame, source_label: str) -> pd.DataFrame:
    """Sort by the canonical columns if present."""
    missing = [c for c in SORT_COLUMNS if c not in df.columns]
    if missing:
        print(f"⚠️ {source_label}: Missing expected columns for sort: {missing} — leaving order unchanged.")
        return df
    # mergesort is stable; keeps deterministic ordering for ties.
    return df.sort_values(SORT_COLUMNS, kind="mergesort")

# ============================================================
# 1️⃣ COPY CSV FROM LEGACY PIPELINE
# ============================================================

csv_source_path = CSV_SOURCE_DIR / CSV_NAME
csv_dest_path   = OUTPUT_DIR / "CSV.CSV"

if not csv_source_path.exists():
    raise FileNotFoundError(f"❌ CSV not found: {csv_source_path}")

df_csv = pd.read_csv(csv_source_path)
drop_cols_csv = [c for c in ["id", *COLUMNS_TO_DROP] if c in df_csv.columns]
if drop_cols_csv:
    df_csv = df_csv.drop(columns=drop_cols_csv)
df_csv = sorted_or_warn(df_csv, "CSV")
df_csv.to_csv(csv_dest_path, index=False)

print(f"📄 Copied CSV → {csv_dest_path} (rows: {len(df_csv):,})")

# ============================================================
# 2️⃣ EXPORT DB TABLE TO CSV
# ============================================================

if not DB_PATH.exists():
    raise FileNotFoundError(f"❌ DB not found: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)

try:
    df_db = pd.read_sql_query(
        f"SELECT * FROM {DB_TABLE_NAME};",
        conn
    )
finally:
    conn.close()

db_csv_path = OUTPUT_DIR / "DB.csv"
df_db = sorted_or_warn(df_db, "DB")
drop_cols_db = [c for c in ["id", *COLUMNS_TO_DROP] if c in df_db.columns]
if drop_cols_db:
    df_db = df_db.drop(columns=drop_cols_db)
df_db.to_csv(db_csv_path, index=False)

print(f"📄 Exported DB table → {db_csv_path} (rows: {len(df_db):,})")

# ============================================================
# SUMMARY
# ============================================================

print("\n✅ Debug snapshot complete")
