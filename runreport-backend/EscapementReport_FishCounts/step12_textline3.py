"""
step12_textline3.py
-----------------------------------------
Build TL3 column in Escapement_PlotPipeline.

Rules:
    • TL3 only active if TL2 is non-empty
    • TL3 = last 11 tokens from TL2 that match:
          - numbers (with optional commas)
          - dash ("-")
    • Examples:
        TL2 = "... 41 3 - - - 44 - - - - -" 
            → TL3 = "41 3 - - - 44 - - - - -"

Writes TL3 back into local.db → Escapement_PlotPipeline.
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
# Ensure TL3 column exists
# ------------------------------------------------------------

def ensure_tl3_column():
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN TL3 TEXT")
            conn.commit()
            print("   ✔ Added TL3 column")
        except sqlite3.OperationalError:
            print("   ℹ️ TL3 column already exists")

# ------------------------------------------------------------
# Helper: extract TL3 from TL2
# ------------------------------------------------------------

def extract_TL3(tl2: str) -> str:
    """Return last 11 numeric/dash tokens from TL2."""
    if not isinstance(tl2, str) or tl2.strip() == "":
        return ""

    # Grab all tokens that are:
    #   - numbers (possibly with commas)
    #   - dash '-'
    tokens = re.findall(r"\d[\d,]*|-", tl2)

    if not tokens:
        return ""

    # Take last 11 tokens
    selected = tokens[-11:]
    return " ".join(selected)

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    print("🏗️ Step 12: Creating TL3...")

    ensure_tl3_column()

    # Load needed columns
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, TL2 FROM Escapement_PlotPipeline ORDER BY id"
        ).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []
    for row in rows:
        tl2 = row["TL2"]
        tl3 = extract_TL3(tl2)
        updates.append((tl3, row["id"]))

    print(f"📝 Updating TL3 for {len(updates):,} rows...")

    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET TL3 = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print("✅ Step 12 complete — TL3 column populated.")


if __name__ == "__main__":
    main()