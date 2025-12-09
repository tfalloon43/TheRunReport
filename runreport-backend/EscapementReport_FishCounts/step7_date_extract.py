"""
step7_date_extract.py
-----------------------------------------
Extract dates from text_line inside Escapement_PlotPipeline.

Logic mirrors old CSV script:

    • Look for MM/DD/YY or MM/DD/YYYY inside text_line
    • Store extracted date in a new column: date
    • Writes updates back into SQLite table Escapement_PlotPipeline
"""

import sqlite3
import re
from pathlib import Path

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

CURRENT_DIR = Path(__file__).resolve().parent          # .../EscapementReport_FishCounts
BACKEND_ROOT = CURRENT_DIR.parent                      # .../runreport-backend
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

print(f"🗄️ Using DB: {DB_PATH}")

# ------------------------------------------------------------
# DB helper
# ------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------------------------------------------------
# Date extraction regex
# ------------------------------------------------------------

DATE_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})")

def extract_date(text: str):
    if not isinstance(text, str):
        return None
    m = DATE_RE.search(text)
    return m.group(1) if m else None


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    print("📅 Step 7: Extracting dates into column 'date'...")

    with get_conn() as conn:

        # Ensure column exists
        conn.execute("""
            ALTER TABLE Escapement_PlotPipeline
            ADD COLUMN date TEXT
        """)
        conn.commit()

    # Load rows after adding column
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, text_line
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []
    for r in rows:
        date_val = extract_date(r["text_line"])
        updates.append((date_val, r["id"]))

    print(f"📝 Dates extracted for {len(updates):,} rows")

    # Write updates in one transaction
    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET date = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print("✅ Step 7 complete — date column updated.")


if __name__ == "__main__":
    main()