"""
step28_pdf_date.py
-----------------------------------------
Extract pdf_date (ISO format) from pdf_name.

Example:
    pdf_name = "WA_EscapementReport_01-02-2014.pdf"
    pdf_date = "2014-01-02"

Updates table: Escapement_PlotPipeline
Adds/overwrites column: pdf_date
"""

import sqlite3
import pandas as pd
import re
from pathlib import Path

print("🏗️ Step 28: Extracting pdf_date (ISO) from pdf_name...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
# File location:
#   runreport-backend/EscapementReport_FishCounts/step28_pdf_date.py
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent              # runreport-backend/
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

print(f"🗄️ Using DB: {DB_PATH}")

# ------------------------------------------------------------
# Extract ISO pdf_date from pdf_name
# ------------------------------------------------------------
def extract_pdf_date(pdf_name):
    """Return YYYY-MM-DD extracted from filename like MM-DD-YYYY."""
    if not isinstance(pdf_name, str):
        return ""
    match = re.search(r"(\d{2})-(\d{2})-(\d{4})", pdf_name)
    if not match:
        return ""
    mm, dd, yyyy = match.groups()
    return f"{yyyy}-{mm}-{dd}"

def main():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline", conn)

    print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

    df["pdf_date"] = df["pdf_name"].apply(extract_pdf_date)
    parsed_dates = pd.to_datetime(df["pdf_date"], errors="coerce").dt.date
    df["pdf_date"] = parsed_dates.astype(str).where(parsed_dates.notna(), "")

    df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
    conn.close()

    nonblank = df["pdf_date"].astype(str).str.strip().ne("").sum()
    print(f"✅ pdf_date extraction complete")
    print(f"📊 {nonblank} of {len(df):,} rows populated with valid pdf_date values")
    print("🎯 Example format: 2014-01-02")
    print("🔄 Escapement_PlotPipeline updated in local.db")


if __name__ == "__main__":
    main()
