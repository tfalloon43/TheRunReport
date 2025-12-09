"""
step21_date_iso.py
------------------------------------------------------------
Normalize the "date" column in Escapement_PlotPipeline
(MM/DD/YY or MM/DD/YYYY) into ISO format (YYYY-MM-DD).

Writes results into a new column:
    date_iso

• Only modifies rows with valid dates.
• Rows with invalid/blank dates become "".
• Updates values directly inside local.db.

Table: Escapement_PlotPipeline
------------------------------------------------------------
"""

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

print(f"🗄️ Using DB → {DB_PATH}")

# ------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_date_iso_column():
    """Add date_iso column if missing."""
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN date_iso TEXT")
            conn.commit()
            print("   ✔ Added date_iso column")
        except sqlite3.OperationalError:
            print("   ℹ️ date_iso column already exists")


# ------------------------------------------------------------
# Date conversion helper
# ------------------------------------------------------------
def convert_to_iso(date_str):
    """Convert MM/DD/YY or MM/DD/YYYY → YYYY-MM-DD."""
    if not isinstance(date_str, str) or not date_str.strip():
        return ""

    date_str = date_str.strip()

    # Try both possible formats
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(date_str, fmt)

            # Fix two-digit future dates → assume 2000s
            if parsed.year < 1950:
                parsed = parsed.replace(year=parsed.year + 2000)

            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return ""


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    print("🏗️ Step 21: Converting date → date_iso...")

    ensure_date_iso_column()

    # Fetch all rows
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, date
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows...")

    updates = []
    filled_count = 0

    for r in rows:
        row_id = r["id"]
        date_val = r["date"] or ""

        iso = convert_to_iso(date_val)
        if iso:
            filled_count += 1

        updates.append((iso, row_id))

    # Apply updates
    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET date_iso = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print("🎉 Step 21 complete!")
    print(f"📊 {filled_count:,} rows converted to ISO format")
    print("🔄 Escapement_PlotPipeline updated with date_iso")


if __name__ == "__main__":
    main()