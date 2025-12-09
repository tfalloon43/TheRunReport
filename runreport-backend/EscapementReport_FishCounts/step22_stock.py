"""
step22_stock.py
------------------------------------------------------------
Extract standalone stock indicator (H/W/U/M/C)
from the end of the Stock_BO column in Escapement_PlotPipeline.

Writes results into:
    Stock

Rules:
    • Only active when Stock_BO is non-empty.
    • Detect final letter H/W/U/M/C at end of string.

------------------------------------------------------------
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

print(f"🗄️ Using DB → {DB_PATH}")

# ------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_stock_column():
    """Add Stock column if missing."""
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN Stock TEXT")
            conn.commit()
            print("   ✔ Added Stock column")
        except sqlite3.OperationalError:
            print("   ℹ️ Stock column already exists")


# ------------------------------------------------------------
# Extraction helper
# ------------------------------------------------------------
STOCK_PATTERN = re.compile(r'(?:\b|[-\s])([HWUMC])\s*$', re.IGNORECASE)

def extract_stock(val):
    """Extract final stock indicator (H/W/U/M/C) from the end of Stock_BO."""
    if not isinstance(val, str) or not val.strip():
        return ""
    match = STOCK_PATTERN.search(val.strip())
    return match.group(1).upper() if match else ""


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    print("🏗️ Step 22: Extracting Stock indicator from Stock_BO...")

    ensure_stock_column()

    # Get Stock_BO + id
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, Stock_BO
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows.")

    updates = []
    filled = 0

    for r in rows:
        row_id = r["id"]
        stock_bo = r["Stock_BO"] or ""

        extracted = extract_stock(stock_bo)
        if extracted:
            filled += 1

        updates.append((extracted, row_id))

    # Write back to DB
    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET Stock = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print(f"🎉 Step 22 complete!")
    print(f"📊 {filled:,} rows populated with Stock indicator")
    print("🔄 Escapement_PlotPipeline updated with Stock column")


if __name__ == "__main__":
    main()