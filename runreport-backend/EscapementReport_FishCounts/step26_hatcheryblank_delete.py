"""
step26_hatcheryblank_delete.py
------------------------------------------------------------
Remove rows from Escapement_PlotPipeline where Hatchery_Name
is blank, NULL, or whitespace.

Equivalent logic to CSV script:
    3_hatchery_blank.py
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

print("🏗️ Step 26: Removing rows with blank Hatchery_Name…")
print(f"🗄️ Using DB → {DB_PATH}")

# ------------------------------------------------------------
# DB Helpers
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------------------------------------------------
# Ensure column exists
# ------------------------------------------------------------
def ensure_hatchery_col():
    with get_conn() as conn:
        cols = conn.execute(
            "PRAGMA table_info(Escapement_PlotPipeline)"
        ).fetchall()
        col_names = {c["name"] for c in cols}

    if "Hatchery_Name" not in col_names:
        raise RuntimeError("❌ Missing required column: Hatchery_Name")

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    ensure_hatchery_col()

    with get_conn() as conn:
        before_count = conn.execute(
            "SELECT COUNT(*) FROM Escapement_PlotPipeline"
        ).fetchone()[0]

        delete_sql = """
            DELETE FROM Escapement_PlotPipeline
            WHERE Hatchery_Name IS NULL
               OR TRIM(Hatchery_Name) = ''
        """

        conn.execute(delete_sql)
        conn.commit()

        after_count = conn.execute(
            "SELECT COUNT(*) FROM Escapement_PlotPipeline"
        ).fetchone()[0]

    removed = before_count - after_count

    print(f"🧹 Removed {removed:,} rows with blank Hatchery_Name.")
    print(f"✅ {after_count:,} rows remain in Escapement_PlotPipeline.")

if __name__ == "__main__":
    main()