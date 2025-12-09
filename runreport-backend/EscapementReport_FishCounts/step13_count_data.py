"""
step13_count_data.py
-----------------------------------------
Convert TL3 into normalized numeric count_data.

Transformation rules:
    • Replace "-" with "0"
    • Remove commas from numbers
    • Keep token order intact
    • Produce a space-separated numeric string

Examples:
    TL3 = "108 4 - - - - - 104 - 8 -"
        → "108 4 0 0 0 0 0 104 0 8 0"

    TL3 = "- - 1,740,000 - - - - - - - -"
        → "0 0 1740000 0 0 0 0 0 0 0 0"

Writes results into local.db → Escapement_PlotPipeline.count_data.
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
# Ensure count_data column exists
# ------------------------------------------------------------

def ensure_countdata_column():
    with get_conn() as conn:
        try:
            conn.execute(
                "ALTER TABLE Escapement_PlotPipeline ADD COLUMN count_data TEXT"
            )
            conn.commit()
            print("   ✔ Added count_data column")
        except sqlite3.OperationalError:
            print("   ℹ️ count_data column already exists")

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------

def normalize_count_data(tl3: str) -> str:
    """
    Convert TL3 string into normalized space-separated numbers:
      • '-' → 0
      • remove commas in numbers
      • keep token order
    """
    if not isinstance(tl3, str) or not tl3.strip():
        return ""

    tokens = re.split(r"\s+", tl3.strip())
    normalized = []

    for t in tokens:
        if t == "-":
            normalized.append("0")
        else:
            cleaned = t.replace(",", "")
            # If cleaned string is numeric, accept it; otherwise treat as 0
            if cleaned.isdigit():
                normalized.append(cleaned)
            else:
                normalized.append("0")

    return " ".join(normalized)

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    print("🏗️ Step 13: Creating count_data...")

    ensure_countdata_column()

    # Fetch TL3 and ids
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, TL3 FROM Escapement_PlotPipeline ORDER BY id"
        ).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []
    populated_count = 0

    for row in rows:
        tl3 = row["TL3"]
        out = normalize_count_data(tl3)
        if out.strip():
            populated_count += 1
        updates.append((out, row["id"]))

    # Write results back to DB
    print("📝 Updating count_data for all rows...")

    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET count_data = ? WHERE id = ?",
            updates
        )
        conn.commit()

    print("✅ Step 13 complete — count_data populated.")
    print(f"📊 Populated rows: {populated_count:,} of {len(rows):,}")


if __name__ == "__main__":
    main()