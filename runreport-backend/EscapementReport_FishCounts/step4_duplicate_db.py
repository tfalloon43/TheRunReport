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
import sys

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"
sys.path.append(str(BACKEND_ROOT))

print(f"🗄️ Using DB: {DB_PATH}")

# ------------------------------------------------------------
# DB helper (local)
# ------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


try:
    from publish.supabase_client import SupabaseConfigError, get_supabase_client
except Exception as e:
    raise ImportError(
        "❌ Could not import Supabase client helper. "
        f"Checked path: {BACKEND_ROOT / 'publish'}\n"
        f"Error: {e}"
    )


def get_supabase():
    try:
        return get_supabase_client()
    except SupabaseConfigError as exc:
        print(f"❌ Supabase not configured: {exc}")
        return None

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
        report_id INTEGER,
        line_order INTEGER,
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


def _fetch_rawlines_page(client, start: int, page_size: int) -> list[dict]:
    response = (
        client.table("EscapementRawLines")
        .select("id,report_id,line_order,pdf_name,page_num,text_line")
        .order("report_id")
        .order("line_order")
        .order("id")
        .range(start, start + page_size - 1)
        .execute()
    )
    if getattr(response, "error", None):
        raise RuntimeError(f"Supabase query failed: {response.error}")
    return response.data or []


def _insert_pipeline_rows(rows: list[dict]) -> None:
    if not rows:
        return
    payload = [
        (
            row.get("report_id"),
            row.get("line_order"),
            row["pdf_name"],
            row["page_num"],
            row["text_line"],
        )
        for row in rows
    ]
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO Escapement_PlotPipeline
                (report_id, line_order, pdf_name, page_num, text_line)
            VALUES (?, ?, ?, ?, ?);
            """,
            payload,
        )
        conn.commit()


def copy_raw_to_pipeline(client):
    """
    Clears Escapement_PlotPipeline and copies ALL rows
    from EscapementRawLines.
    """

    with get_conn() as conn:
        print("🧽 Clearing Escapement_PlotPipeline table...")
        conn.execute("DELETE FROM Escapement_PlotPipeline;")
        conn.commit()

    print("📋 Copying rows from Supabase EscapementRawLines → Escapement_PlotPipeline...")
    page_size = 1000
    start = 0
    total = 0

    while True:
        rows = _fetch_rawlines_page(client, start, page_size)
        if not rows:
            break
        _insert_pipeline_rows(rows)
        total += len(rows)
        start += page_size

    with get_conn() as conn:
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
    client = get_supabase()
    if client is None:
        return
    copy_raw_to_pipeline(client)

    print("\n🎉 Step 4 complete — working copy is ready for transformations.")


if __name__ == "__main__":
    main()
