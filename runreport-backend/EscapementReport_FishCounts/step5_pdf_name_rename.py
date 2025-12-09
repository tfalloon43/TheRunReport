"""
step5_pdf_name_rename.py
-----------------------------------------
Rename pdf_name values inside Escapement_PlotPipeline
based on date patterns in the filename.

Old filesystem script renamed physical PDFs.
New script renames ONLY the database values.

Target table:
    Escapement_PlotPipeline
        id, pdf_name, page_num, text_line

New pdf_name format:
    WA_EscapementReport_MM-DD-YYYY.pdf

Patterns matched:
    1) MMDDYY.pdf          → 010214.pdf
    2) M-D(-letter)?-YYYY  → 11-03b-2022.pdf, 1_7_2021.pdf, etc.
"""

import re
import sqlite3
from datetime import datetime
from pathlib import Path

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
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
# Regex patterns (copied from your filesystem version)
# ------------------------------------------------------------

# Pattern 1 — MMDDYY.pdf (e.g., 010214.pdf)
pattern_mmddyy = re.compile(r"(\d{6})\.pdf$", re.IGNORECASE)

# Pattern 2 — MM-DD(-optional letter)-YYYY
pattern_mmddyyyy = re.compile(
    r"(\d{1,2})[-_](\d{1,2})(?:[A-Za-z])?[-_](\d{4})",
    re.IGNORECASE
)

# ------------------------------------------------------------
# Renaming logic
# ------------------------------------------------------------

def generate_new_name(old_name: str) -> str | None:
    """
    Given the old filename found in pdf_name, return the new standardized name.
    Returns None if no valid date pattern is detected.
    """

    # Case 1 — 010214.pdf → 01-02-2014
    m1 = pattern_mmddyy.search(old_name)
    if m1:
        date_str = m1.group(1)
        try:
            dt = datetime.strptime(date_str, "%m%d%y")
            return f"WA_EscapementReport_{dt.strftime('%m-%d-%Y')}.pdf"
        except ValueError:
            return None

    # Case 2 — 11-03b-2022.pdf → 11-03-2022
    m2 = pattern_mmddyyyy.search(old_name)
    if m2:
        m, d, y = m2.groups()
        try:
            dt = datetime.strptime(f"{m}-{d}-{y}", "%m-%d-%Y")
            return f"WA_EscapementReport_{dt.strftime('%m-%d-%Y')}.pdf"
        except ValueError:
            return None

    # No recognizable date pattern
    return None

# ------------------------------------------------------------
# Main processing
# ------------------------------------------------------------

def main():
    print("🔄 Step 5: Renaming pdf_name values inside Escapement_PlotPipeline...")

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, pdf_name
            FROM Escapement_PlotPipeline
            GROUP BY pdf_name
        """).fetchall()

    print(f"📄 Unique filenames found: {len(rows)}")

    updates = 0

    with get_conn() as conn:
        for row in rows:
            old_name = row["pdf_name"]
            new_name = generate_new_name(old_name)

            if not new_name:
                print(f"🚫 No date pattern matched: {old_name}")
                continue

            if new_name == old_name:
                continue  # already correct format

            conn.execute("""
                UPDATE Escapement_PlotPipeline
                SET pdf_name = ?
                WHERE pdf_name = ?
            """, (new_name, old_name))

            updates += conn.total_changes

            print(f"   ✔ {old_name} → {new_name}")

        conn.commit()

    print(f"\n🎉 Step 5 complete — {updates} rows renamed.")


if __name__ == "__main__":
    main()