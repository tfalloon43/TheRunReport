"""
step18_facility.py
-----------------------------------------
Builds facility column inside Escapement_PlotPipeline.

Logic:
  • Only rows with a valid date are eligible.
  • facility = hatch_name_map[Hatchery_Name] if found (case-insensitive)
  • Otherwise facility = Hatchery_Name.title()
  • Rows without date → facility = ""
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

print(f"🗄️ Using DB: {DB_PATH}")

# ------------------------------------------------------------
# Add project root so lookup_maps is found
# ------------------------------------------------------------
if str(BACKEND_ROOT) not in sys.path:
    sys.path.append(str(BACKEND_ROOT))

from lookup_maps import hatch_name_map   # noqa

# ------------------------------------------------------------
# DB helper
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------------------------------------------------
# Ensure facility column exists
# ------------------------------------------------------------
def ensure_facility_column():
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN facility TEXT")
            conn.commit()
            print("   ✔ Added facility column")
        except sqlite3.OperationalError:
            print("   ℹ️ facility column already exists")

# ------------------------------------------------------------
# Validation helper
# ------------------------------------------------------------
def valid_date(val):
    # Treat any non-empty stringified value as valid, except explicit nan/none markers.
    val = "" if val is None else str(val)
    val = val.strip()
    if not val or val.lower() in ("nan", "none"):
        return False
    return True

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    print("🏗️ Step 18: Building facility column...")

    ensure_facility_column()

    # Load dataset
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, date, Hatchery_Name
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []

    for r in rows:
        row_id = r["id"]
        date_val = (r["date"] or "").strip()
        hatch_val = (r["Hatchery_Name"] or "").strip()

        # If no date → facility = ""
        if not valid_date(date_val):
            facility = ""
        else:
            if not hatch_val:
                facility = ""
            else:
                # case-insensitive lookup
                lookup_key = hatch_val.upper()
                facility = hatch_name_map.get(lookup_key, hatch_val.title()).strip()

        updates.append((facility, row_id))

    # Write facility values back to DB
    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET facility = ? WHERE id = ?",
            updates
        )
        conn.commit()

    populated = sum(1 for f, _ in updates if f.strip())

    print("\n🎉 Step 18 complete!")
    print(f"📊 {populated:,} of {len(rows):,} rows populated with facility names")
    print("🔄 Escapement_PlotPipeline updated")

if __name__ == "__main__":
    main()
