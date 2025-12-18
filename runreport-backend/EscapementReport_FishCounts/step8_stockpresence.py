"""
step8_stockpresence.py
-----------------------------------------
Adds a 'stock_presence' column to Escapement_PlotPipeline.

Logic matches old Step 3:

    • If a row has a date (not null/empty)
    • Search text_line for standalone stock letter:
        H, W, U, M, C
    • Store result in stock_presence
"""

import sqlite3
import re
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
# Regex: standalone H/W/M/U/C
# ------------------------------------------------------------

STOCK_RE = re.compile(r"\b([HWUMC])\b")

def find_stock_indicator(text: str):
    if not isinstance(text, str):
        return None
    m = STOCK_RE.search(text)
    return m.group(1) if m else None

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    print("🐟 Step 8: Adding stock_presence column...")

    # 1) Ensure column exists
    with get_conn() as conn:
        conn.execute("""
            ALTER TABLE Escapement_PlotPipeline
            ADD COLUMN stock_presence TEXT
        """)
        conn.commit()

    # 2) Load rows
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, text_line, date
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []

    # 3) Apply detection logic
    for r in rows:
        date_val = r["date"]
        text = r["text_line"]

        stock = None
        if date_val is not None and str(date_val).strip() != "":
            stock = find_stock_indicator(text)

        updates.append((stock, r["id"]))

    print(f"📝 Computed stock_presence for {len(updates):,} rows")

    # 4) Write back to DB
    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET stock_presence = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print("✅ Step 8 complete — stock_presence column updated.")


if __name__ == "__main__":
    main()