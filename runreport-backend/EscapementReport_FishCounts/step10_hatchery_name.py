"""
step10_hatchery_name.py
-----------------------------------------
Extract hatchery names from text_line into Hatchery_Name column.

Rules (matches old Step 5):
    • Capture ≥2 consecutive ALL-CAPS words at start of line.
    • Stop when encountering a lowercase-starting word or location tokens:
          I- , SR- , HWY , US- , STOCK-
    • Fix glued patterns like "HATCHERYPriest"
    • Ignore WDFW, CAUTION, species headers (e.g., Fall Chinook)
    • Apply lookup_maps.hatchery_name_corrections

Writes results into Escapement_PlotPipeline in local.db.
"""

import sqlite3
import re
from pathlib import Path
import sys

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

LOOKUP_PATH = BACKEND_ROOT / "lookup_maps.py"

print(f"🗄️ Using DB: {DB_PATH}")

# ------------------------------------------------------------
# Ensure lookup_maps importable
# ------------------------------------------------------------

project_root_str = str(BACKEND_ROOT.resolve())
if project_root_str not in sys.path:
    sys.path.append(project_root_str)

try:
    import lookup_maps
    corrections = lookup_maps.hatchery_name_corrections
except Exception as e:
    raise RuntimeError(f"❌ Could not import lookup_maps.py: {e}")

# ------------------------------------------------------------
# DB helper
# ------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------------------------------------------------
# Hatchery extraction logic (from old Step 5)
# ------------------------------------------------------------

STOPWORD_RE = re.compile(r"^(I-|SR-|HWY|US-|STOCK-)", re.IGNORECASE)

def extract_hatchery_name(text: str):
    """
    Extracts ≥2 ALL-CAPS words at the start of text_line, obeying rules
    from the original Step 5.
    """
    if not isinstance(text, str) or not text.strip():
        return ""

    line = text.strip()

    # Fix glued forms like HATCHERYPriest
    line = re.sub(r"(HATCHERY)(?=[A-Z]?[a-z])", r"\1 ", line)
    line = re.sub(
        r"(HATCHERYP|HATCHERYF|HATCHERYR)(?=[A-Z]?[a-z])",
        lambda m: m.group(1)[:-1] + " " + m.group(1)[-1],
        line,
    )

    # Skip irrelevant headers
    if re.match(r"^(WDFW|CAUTION)\b", line, re.IGNORECASE):
        return ""
    if re.match(r"^[A-Z][a-z]+\s+Chinook", line):
        return ""

    words = line.split()
    capture = []

    for w in words:
        # stop when lowercase-starting or location prefix
        if re.match(r"^[A-Z][a-z]", w):
            break
        if STOPWORD_RE.match(w):
            break

        # Only uppercase tokens
        if re.match(r"^[A-Z0-9&'()./-]+$", w):
            capture.append(w)
        else:
            break

    if len(capture) >= 2:
        return " ".join(capture)

    return ""

def apply_corrections(name: str):
    if not isinstance(name, str) or not name.strip():
        return name
    return corrections.get(name.strip(), name.strip())

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    print("🏗️ Step 10: Extracting Hatchery_Name...")

    # 1) Ensure column exists
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN Hatchery_Name TEXT")
            conn.commit()
            print("   ✔ Added Hatchery_Name column")
        except sqlite3.OperationalError:
            print("   ℹ️ Hatchery_Name column already exists")

    # 2) Load rows
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, text_line
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    updates = []

    # 3) Extract names
    for row in rows:
        name = extract_hatchery_name(row["text_line"])
        name = apply_corrections(name)
        if name:
            updates.append((name, row["id"]))

    print(f"📝 Will update {len(updates):,} rows with Hatchery_Name")

    # 4) Write updates
    with get_conn() as conn:
        conn.executemany("""
            UPDATE Escapement_PlotPipeline
            SET Hatchery_Name = ?
            WHERE id = ?
        """, updates)
        conn.commit()

    print("✅ Step 10 complete — Hatchery_Name extracted and added.")


if __name__ == "__main__":
    main()