"""
step25_dateblank_delete.py
------------------------------------------------------------
Remove rows from Escapement_PlotPipeline where the date field
('date_iso' if available, otherwise 'date') is blank or NULL.

Equivalent logic to old CSV-based:
    2_date_blank.py
"""

import sqlite3
from pathlib import Path

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

print(f"🏗️ Step 25: Removing rows with blank date/date_iso…")
print(f"🗄️ Using DB → {DB_PATH}")

# ------------------------------------------------------------
# DB Helpers
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------------------------------------------------
# Determine which date column to use
# ------------------------------------------------------------
def determine_date_col():
    with get_conn() as conn:
        cols = conn.execute(
            "PRAGMA table_info(Escapement_PlotPipeline)"
        ).fetchall()

    col_names = {c["name"] for c in cols}

    if "date_iso" in col_names:
        return "date_iso"
    elif "date" in col_names:
        return "date"
    else:
        raise RuntimeError("❌ Neither 'date_iso' nor 'date' column exists in Escapement_PlotPipeline.")

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    date_col = determine_date_col()
    print(f"📅 Using date column: {date_col}")

    with get_conn() as conn:
        before_count = conn.execute(
            "SELECT COUNT(*) FROM Escapement_PlotPipeline"
        ).fetchone()[0]

        # Delete rows where date field is NULL, empty, or whitespace
        delete_sql = f"""
            DELETE FROM Escapement_PlotPipeline
            WHERE {date_col} IS NULL
               OR TRIM({date_col}) = ''
        """

        conn.execute(delete_sql)
        conn.commit()

        after_count = conn.execute(
            "SELECT COUNT(*) FROM Escapement_PlotPipeline"
        ).fetchone()[0]

    removed = before_count - after_count

    print(f"🧹 Removed {removed:,} rows with blank {date_col}.")
    print(f"✅ {after_count:,} rows remain in Escapement_PlotPipeline.")

if __name__ == "__main__":
    main()