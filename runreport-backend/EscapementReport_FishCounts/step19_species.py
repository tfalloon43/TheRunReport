"""
step19_species.py
-----------------------------------------
Builds 'species' column inside Escapement_PlotPipeline using
the species_headers list from lookup_maps.py.

Rules:
  • When a text_line matches (case-insensitive) a species header,
      set that as the current species.
  • All following rows inherit it until a new header is found.
  • Species column gets the exact-cased version from species_headers.
  • Rows before the first header remain blank.
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

print(f"🗄️ Using DB at: {DB_PATH}")

# ------------------------------------------------------------
# Import species_headers
# ------------------------------------------------------------
if str(BACKEND_ROOT) not in sys.path:
    sys.path.append(str(BACKEND_ROOT))

from lookup_maps import species_headers   # noqa

# Prepare lookup dictionary (case-insensitive)
species_lookup = {s.lower(): s for s in species_headers}

# ------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_species_column():
    """Make sure 'species' column exists."""
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN species TEXT")
            conn.commit()
            print("   ✔ Added species column")
        except sqlite3.OperationalError:
            print("   ℹ️ species column already exists")

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    print("🏗️ Step 19: Assigning species using species_headers...")

    ensure_species_column()

    # Load rows ordered by id (important for inheritance)
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, text_line
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []
    current_species = ""

    for r in rows:
        row_id = r["id"]
        text = (r["text_line"] or "").strip()
        key = text.lower()

        # Check if this line *is* a species header
        if key in species_lookup:
            current_species = species_lookup[key]   # exact-cased version
            updates.append((current_species, row_id))
        else:
            # Inherit prior species (or empty before first match)
            updates.append((current_species, row_id))

    # Write back to SQLite
    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET species = ? WHERE id = ?",
            updates,
        )
        conn.commit()

    populated = sum(1 for species, _ in updates if species.strip())

    print("\n🎉 Step 19 complete!")
    print(f"📊 {populated:,} rows populated with species")
    print(f"🔄 Escapement_PlotPipeline updated")

if __name__ == "__main__":
    main()