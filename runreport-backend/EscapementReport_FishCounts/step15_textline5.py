"""
step15_textline5.py
-----------------------------------------
Create TL5 column:

TL5 logic:
    • If stock_presence_lower has a non-blank value → TL5 = text_line
    • Otherwise → TL5 = ""

Writes TL5 back into Escapement_PlotPipeline.
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
# Ensure TL5 column exists
# ------------------------------------------------------------

def ensure_tl5_column():
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN TL5 TEXT")
            conn.commit()
            print("   ✔ Added TL5 column")
        except sqlite3.OperationalError:
            print("   ℹ️ TL5 column already exists")

# ------------------------------------------------------------
# Generate TL5 values
# ------------------------------------------------------------

def make_tl5(text_line: str, spl: str) -> str:
    """
    TL5 = text_line if stock_presence_lower exists and is non-empty.
    Otherwise TL5 = "".
    """
    if isinstance(spl, str) and spl.strip():
        return str(text_line).strip()
    return ""

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    print("🏗️ Step 15: Creating TL5...")

    ensure_tl5_column()

    # Load required values
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, text_line, stock_presence_lower FROM Escapement_PlotPipeline ORDER BY id"
        ).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []
    populated = 0

    for r in rows:
        text_line = r["text_line"]
        spl = r["stock_presence_lower"]

        tl5 = make_tl5(text_line, spl)

        if tl5.strip():
            populated += 1

        updates.append((tl5, r["id"]))

    # Write TL5 back to database
    print("📝 Updating TL5 values...")

    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET TL5 = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print("✅ Step 15 complete — TL5 created.")
    print(f"📊 Populated rows: {populated:,} of {len(rows):,}")


if __name__ == "__main__":
    main()