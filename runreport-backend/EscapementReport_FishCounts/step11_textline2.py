"""
step11_textline2.py
-----------------------------------------
Create TL2 column in Escapement_PlotPipeline.

Rules:
    • TL2 only active if row has a date
    • TL2 = text_line with:
          1. Hatchery_Name removed (only at beginning)
          2. date AND everything after it removed
    • Everything BEFORE Hatchery_Name stays

Writes results back into local.db → Escapement_PlotPipeline.
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
# Make sure TL2 column exists
# ------------------------------------------------------------

def ensure_tl2_column():
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN TL2 TEXT")
            conn.commit()
            print("   ✔ Added TL2 column")
        except sqlite3.OperationalError:
            print("   ℹ️ TL2 column already exists")

# ------------------------------------------------------------
# TL2 builder (same logic as old 6_TL2.py)
# ------------------------------------------------------------

def build_tl2(text, hatchery, date):
    if not isinstance(text, str):
        text = ""
    if not isinstance(hatchery, str):
        hatchery = ""
    if not isinstance(date, str):
        date = ""

    text = text.strip()
    hatchery = hatchery.strip()
    date = date.strip()

    # TL2 only active if a date exists
    if not date or date.lower() == "nan":
        return ""

    # 1 — remove hatchery name at beginning
    if hatchery:
        pattern = re.escape(hatchery)
        text = re.sub(rf"^{pattern}\s*", "", text).strip()

    # 2 — remove date and everything after it
    text = re.sub(rf"\s*{re.escape(date)}.*$", "", text).strip()

    return text

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    print("🏗️ Step 11: Building TL2 column...")

    ensure_tl2_column()

    # Load entire table
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, text_line, Hatchery_Name, date FROM Escapement_PlotPipeline ORDER BY id"
        ).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []

    for row in rows:
        tl2 = build_tl2(row["text_line"], row["Hatchery_Name"], row["date"])
        updates.append((tl2, row["id"]))

    print(f"📝 Updating TL2 for {len(updates):,} rows...")

    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET TL2 = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print("✅ Step 11 complete — TL2 column populated.")


if __name__ == "__main__":
    main()