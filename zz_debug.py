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
import re
from datetime import datetime
from pathlib import Path

print("🧪 zz_debug.py — CSV vs DB snapshot tool")

# ============================================================
# 🔧🔧🔧 MANUAL EDIT SECTION (CHANGE THESE)
# ============================================================

# --- CSV produced by 0_master_pipeline ---
CSV_NAME = "csv_plotdata.csv"     # 👈 CHANGE THIS AS NEEDED

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

# Columns to remove from both outputs (set to [] to keep all). Example: ["species"]
COLUMNS_TO_DROP = [
#                   "species", 
#                   "stock_presence", 
#                   "stock_presence_lower",
#                   "Hatchery_Name",
#                   "Family",
#                   "facility",
#                   "pdf_name",
#                   "pdf_date",
                   "x_count",
                   "by_short",
                   "by_adult_length",
                   "by_adult",
                   "adult_diff",
                   "day_diff",
                   "x_count2",
                   "by_short2",
                   "by_adult2_length",
                   "by_adult2",
                   "adult_diff2",
                   "day_diff2",
#                   "x_count3",
#                   "by_short3",
                   "by_adult3_length",
                   "by_adult3",
                   "adult_diff3",
                   "day_diff3",
                   "",
                   "",
                   "",]

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def convert_to_iso(date_str):
    """Convert MM/DD/YY, MM/DD/YYYY, or ISO-with-time → YYYY-MM-DD."""
    if date_str is None:
        return ""
    if not isinstance(date_str, str):
        date_str = str(date_str)
    if not date_str.strip():
        return ""

    # Remove any time component
    date_str = date_str.strip()
    for sep in (" ", "T"):
        if sep in date_str:
            date_str = date_str.split(sep)[0]
            break

    # Already ISO? keep just date part
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return ""

    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(date_str, fmt)
            if parsed.year < 1950:
                parsed = parsed.replace(year=parsed.year + 2000)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return ""

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
drop_cols_db = [c for c in ["id", *COLUMNS_TO_DROP] if c in df_db.columns]
if drop_cols_db:
    df_db = df_db.drop(columns=drop_cols_db)
if "date_iso" in df_db.columns:
    df_db["date_iso"] = df_db["date_iso"].apply(convert_to_iso)
df_db.to_csv(db_csv_path, index=False)

print(f"📄 Exported DB table → {db_csv_path} (rows: {len(df_db):,})")

# ============================================================
# SUMMARY
# ============================================================

print("\n✅ Debug snapshot complete")
