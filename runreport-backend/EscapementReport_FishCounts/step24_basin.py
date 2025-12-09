"""
step24_basin.py
------------------------------------------------------------
Builds the 'basin' column inside Escapement_PlotPipeline using
basin_map from lookup_maps.py.

Logic:
    • Lookup Hatchery_Name (case-insensitive) in basin_map.
    • If found → assign basin.
    • If blank or not found → basin = "".

Updates table:
    Escapement_PlotPipeline
"""

import sqlite3
from pathlib import Path
import sys

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent            # runreport-backend/
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

# Add project root for lookup_maps import
ROOT = str(BACKEND_ROOT.resolve())
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Import basin_map
try:
    from lookup_maps import basin_map
    print("✅ Imported basin_map from lookup_maps.py")
except Exception as e:
    raise RuntimeError(f"❌ Could not import basin_map: {e}")

print(f"🗄️ Using DB → {DB_PATH}")


# ------------------------------------------------------------
# DB Helpers
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column():
    """Add basin column if it does not exist."""
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN basin TEXT")
            print("   ✔ Added column 'basin'")
        except sqlite3.OperationalError:
            # Column already exists
            pass
        conn.commit()


# ------------------------------------------------------------
# Main logic
# ------------------------------------------------------------
def lookup_basin(hatch_name: str):
    """Case-insensitive basin lookup."""
    if not isinstance(hatch_name, str) or not hatch_name.strip():
        return ""
    return basin_map.get(hatch_name.strip().upper(), "").strip()


def main():
    print("🏗️ Step 24: Assigning basin from Hatchery_Name...")

    ensure_column()

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, Hatchery_Name
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"📄 Loaded {len(rows):,} rows")

    updates = []
    filled_count = 0

    for r in rows:
        row_id = r["id"]
        hatch = r["Hatchery_Name"]
        basin_val = lookup_basin(hatch)

        if basin_val:
            filled_count += 1

        updates.append((basin_val, row_id))

    # Write updates to SQLite
    with get_conn() as conn:
        for basin_val, row_id in updates:
            conn.execute(
                "UPDATE Escapement_PlotPipeline SET basin = ? WHERE id = ?",
                (basin_val, row_id)
            )
        conn.commit()

    print("🎉 Step 24 complete!")
    print(f"📊 {filled_count:,} rows assigned basin names")
    print("🔄 Escapement_PlotPipeline updated with basin column")


if __name__ == "__main__":
    main()