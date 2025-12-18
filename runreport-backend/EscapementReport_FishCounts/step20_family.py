"""
step20_family.py
-----------------------------------------
Builds 'Family' column inside Escapement_PlotPipeline using
family_map from lookup_maps.py.

Logic:
  • For each row, look up its species (case-insensitive) in family_map.
  • If found → Family = mapped family
  • If blank or missing → Family = ""
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
# Import lookup_maps.family_map
# ------------------------------------------------------------
if str(BACKEND_ROOT) not in sys.path:
    sys.path.append(str(BACKEND_ROOT))

from lookup_maps import family_map   # noqa

# Normalize lookup (case-insensitive keys)
fam_lookup = {k.lower(): v for k, v in family_map.items()}

# ------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_family_column():
    """Create Family column if not present."""
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN Family TEXT")
            conn.commit()
            print("   ✔ Added Family column")
        except sqlite3.OperationalError:
            print("   ℹ️ Family column already exists")

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    print("🏗️ Step 20: Assigning Family from species...")

    ensure_family_column()

    # Load species + row IDs
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, species
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []
    filled_count = 0

    for r in rows:
        row_id = r["id"]
        species = (r["species"] or "").strip()

        if not species:
            updates.append(("", row_id))
            continue

        fam = fam_lookup.get(species.lower(), "")
        if fam:
            filled_count += 1

        updates.append((fam, row_id))

    # Write back
    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET Family = ? WHERE id = ?",
            updates,
        )
        conn.commit()

    print("\n🎉 Step 20 complete!")
    print(f"📊 {filled_count:,} rows assigned a Family value")
    print(f"🔄 Escapement_PlotPipeline updated")

if __name__ == "__main__":
    main()