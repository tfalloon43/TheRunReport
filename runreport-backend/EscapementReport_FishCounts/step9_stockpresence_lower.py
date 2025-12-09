"""
step9_stockpresence_lower.py
-----------------------------------------
Adds `stock_presence_lower` to Escapement_PlotPipeline.

Logic (matches old Step 4):
    • If a row has a date but NO stock_presence
    • Look at the NEXT row's text_line
    • If it has a standalone stock indicator (H/W/U/M/C)
    • Assign that indicator to stock_presence_lower of the *next row*
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
# Regex for standalone stock indicators
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
    print("🐟 Step 9: Adding stock_presence_lower column...")

    # 1) Ensure column exists
    with get_conn() as conn:
        conn.execute("""
            ALTER TABLE Escapement_PlotPipeline
            ADD COLUMN stock_presence_lower TEXT
        """)
        conn.commit()

    # 2) Load all rows ordered by id
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, text_line, date, stock_presence
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []

    # 3) Process sequential rows
    for i in range(len(rows) - 1):
        current = rows[i]
        nxt = rows[i + 1]

        has_date = current["date"] not in (None, "", "nan")
        has_stock = current["stock_presence"] not in (None, "", "nan")

        if has_date and not has_stock:
            next_indicator = find_stock_indicator(nxt["text_line"])
            if next_indicator:
                updates.append((next_indicator, nxt["id"]))

    print(f"📝 Will update {len(updates):,} rows with stock_presence_lower")

    # 4) Execute DB updates
    with get_conn() as conn:
        conn.executemany("""
            UPDATE Escapement_PlotPipeline
            SET stock_presence_lower = ?
            WHERE id = ?
        """, updates)
        conn.commit()

    print("✅ Step 9 complete — stock_presence_lower added.")


if __name__ == "__main__":
    main()