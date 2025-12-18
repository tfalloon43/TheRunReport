"""
step4_duplicate_db.py
-----------------------------------------
Make a full copy of EscapementRawLines into a new table:

    Escapement_PlotPipeline

We do this because:
    • EscapementRawLines = RAW immutable text lines from PDFs
    • Escapement_PlotPipeline = working copy used for parsing,
      grouping, cleaning, row-context logic, etc.

This ensures we NEVER lose raw PDF lines, while downstream
transformations can freely modify the working table.
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

print(f"🗄️ Using DB: {DB_PATH}")

# ------------------------------------------------------------
# DB helper
# ------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------------------------------------------------
# Main duplication logic
# ------------------------------------------------------------

def ensure_plotpipeline_table(recreate=False):
    """
    Create the destination table. If recreate=True, drop any existing
    version first so we always start with a clean duplicate target.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS Escapement_PlotPipeline (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pdf_name TEXT,
        page_num INTEGER,
        text_line TEXT
    );
    """
    with get_conn() as conn:
        if recreate:
            print("♻️ Recreating Escapement_PlotPipeline table...")
            conn.execute("DROP TABLE IF EXISTS Escapement_PlotPipeline;")

        conn.execute(sql)
        conn.commit()


def copy_raw_to_pipeline():
    """
    Clears Escapement_PlotPipeline and copies ALL rows
    from EscapementRawLines.
    """

    with get_conn() as conn:
        # Check source exists
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()

        table_names = {row["name"] for row in tables}
        if "EscapementRawLines" not in table_names:
            raise RuntimeError("❌ Source table EscapementRawLines does not exist!")

        print("🧽 Clearing Escapement_PlotPipeline table...")
        conn.execute("DELETE FROM Escapement_PlotPipeline;")

        print("📋 Copying rows from EscapementRawLines → Escapement_PlotPipeline...")

        conn.execute("""
            INSERT INTO Escapement_PlotPipeline (pdf_name, page_num, text_line)
            SELECT pdf_name, page_num, text_line
            FROM EscapementRawLines;
        """)

        conn.commit()

        # Count rows for confirmation
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM Escapement_PlotPipeline;"
        ).fetchone()["c"]

        print(f"✅ Copy complete — {count:,} rows copied.")


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    print("🔄 Step 4: Duplicating raw PDF table into working table...")

    ensure_plotpipeline_table(recreate=True)
    copy_raw_to_pipeline()

    print("\n🎉 Step 4 complete — working copy is ready for transformations.")


if __name__ == "__main__":
    main()
