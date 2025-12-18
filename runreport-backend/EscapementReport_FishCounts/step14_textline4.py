"""
step14_textline4.py
-----------------------------------------
Build TL4 column from TL2 + TL3.

TL4 = TL2 with the final 11 numeric/dash tokens removed.
This removes the numeric tail while preserving prefix text
(e.g., "Stock-" stays intact).

Writes TL4 back into:
    Escapement_PlotPipeline.TL4
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
# Ensure TL4 column exists
# ------------------------------------------------------------

def ensure_tl4_column():
    with get_conn() as conn:
        try:
            conn.execute(
                "ALTER TABLE Escapement_PlotPipeline ADD COLUMN TL4 TEXT"
            )
            conn.commit()
            print("   ✔ Added TL4 column")
        except sqlite3.OperationalError:
            print("   ℹ️ TL4 column already exists")

# ------------------------------------------------------------
# TL4 generator
# ------------------------------------------------------------

def make_TL4(tl2: str, tl3: str) -> str:
    """
    Remove only the last 11 numeric/dash tokens from TL2.
    Keep everything else intact.
    """
    if not isinstance(tl2, str) or not tl2.strip():
        return ""
    if not isinstance(tl3, str) or not tl3.strip():
        return ""

    # Standardize for token counting
    t2 = tl2.strip().replace(",", "")
    tokens = t2.split()

    # Defensive: if fewer than 11 tokens, do not modify
    if len(tokens) <= 11:
        return tl2.strip()

    # Remove numeric tail (last 11 tokens)
    tl4_tokens = tokens[:-11]
    result = " ".join(tl4_tokens)

    # Only remove trailing whitespace, not hyphens or punctuation
    result = re.sub(r"\s+$", "", result)

    return result

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    print("🏗️ Step 14: Creating TL4...")

    ensure_tl4_column()

    # Load TL2 + TL3
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, TL2, TL3 FROM Escapement_PlotPipeline ORDER BY id"
        ).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []
    populated = 0

    for r in rows:
        tl2 = r["TL2"]
        tl3 = r["TL3"]

        tl4 = make_TL4(tl2, tl3)
        if tl4.strip():
            populated += 1

        updates.append((tl4, r["id"]))

    # Write TL4 back to DB
    print("📝 Updating TL4 values...")

    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET TL4 = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print("✅ Step 14 complete — TL4 created.")
    print(f"📊 Populated rows: {populated:,} of {len(rows):,}")


if __name__ == "__main__":
    main()