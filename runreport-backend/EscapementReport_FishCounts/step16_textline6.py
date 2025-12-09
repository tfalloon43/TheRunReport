"""
step16_textline6.py
-----------------------------------------
Builds TL6 using the same logic as old 11_TL6.py, but now operates
on the Escapement_PlotPipeline table inside local.db.

Rules:
  • Only active if stock_presence_lower has a value.
  • TL6 starts from TL5, keeping everything up to and including
      the first stock indicator (H/W/U/M/C).
  • Cleans leading words (WEIR, HATCHERY, etc.) before Stock-/River-
    (case-sensitive).
  • Removes anything before Stock- or River- if extra junk remains.
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
# Ensure TL6 column exists
# ------------------------------------------------------------

def ensure_tl6_column():
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN TL6 TEXT")
            conn.commit()
            print("   ✔ Added TL6 column")
        except sqlite3.OperationalError:
            print("   ℹ️ TL6 column already exists")

# ------------------------------------------------------------
# Cleanup list (case-sensitive)
# ------------------------------------------------------------

cleanup_before_stock = [
    "WYNOOCHEE R DAM ",
    "WEIR ",
    "HATCHERY ",
    "",
    "",
    "",
    "",
    "",
]

# ------------------------------------------------------------
# Core TL6 transformation
# ------------------------------------------------------------

def make_TL6(tl5: str, spl: str) -> str:
    """
    Build TL6 using old 11_TL6.py logic.
    """
    if not isinstance(spl, str) or not spl.strip():
        return ""
    if not isinstance(tl5, str) or not tl5.strip():
        return ""

    # Step 1 — keep TL5 up to and including first stock letter
    match = re.search(r"\b([HWUMC])\b", tl5)
    if match:
        tl6 = tl5[: match.end()].strip()
    else:
        tl6 = tl5.strip()

    # Step 2 — remove known junk before Stock-/River-
    cleanup_pattern = r"^(?:" + "|".join(map(re.escape, cleanup_before_stock)) + r")+(?=(Stock-|River-))"
    tl6 = re.sub(cleanup_pattern, "", tl6).strip()

    # Step 3 — if leftover junk, isolate beginning at Stock-/River-
    if re.search(r"\b(Stock-|River-)", tl6):
        tl6 = re.sub(r"^.*?\b(Stock-|River-)", r"\1", tl6).strip()

    return tl6

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    print("🏗️ Step 16: Creating TL6...")

    ensure_tl6_column()

    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, TL5, stock_presence_lower FROM Escapement_PlotPipeline ORDER BY id"
        ).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []
    populated = 0

    for r in rows:
        tl5 = r["TL5"]
        spl = r["stock_presence_lower"]

        tl6 = make_TL6(tl5, spl)

        if tl6.strip():
            populated += 1

        updates.append((tl6, r["id"]))

    # Write data back into DB
    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET TL6 = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print("✅ Step 16 complete — TL6 created.")
    print(f"📊 Populated rows: {populated:,} of {len(rows):,}")


if __name__ == "__main__":
    main()