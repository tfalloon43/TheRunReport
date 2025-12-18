"""
step6_removeFISE.py
-----------------------------------------
Remove lines in Escapement_PlotPipeline where:

1. text_line contains "Final in-season estimate" (case-insensitive)
2. AND remove the associated count row:
     • If the FISE row contains a numeric count sequence → delete that row only.
     • Otherwise → search up to 5 rows above on the same pdf_name + page_num
       for a numeric count row and delete that row too.

This mimics the old CSV-based script but operates directly inside SQLite.
"""

import sqlite3
import re
from pathlib import Path
from datetime import datetime

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

print(f"🗄️ Using DB: {DB_PATH}")

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# Regex for count rows (same as old script)
count_pattern = re.compile(
    r'(?:\d{1,3}(?:,\d{3})?|-)(?:\s+(?:\d{1,3}(?:,\d{3})?|-)){4,}'
)

def has_count_sequence(text: str) -> bool:
    if not isinstance(text, str):
        return False
    return bool(count_pattern.search(text))


# ------------------------------------------------------------
# Main logic
# ------------------------------------------------------------

def main():
    print("🧹 Step 6: Removing 'Final in-season estimate' lines and associated count rows...")

    with get_conn() as conn:
        # Load entire working table into memory
        rows = conn.execute("""
            SELECT id, pdf_name, page_num, text_line
            FROM Escapement_PlotPipeline
            ORDER BY pdf_name, page_num, id
        """).fetchall()

    if not rows:
        print("⚠️ No data found in Escapement_PlotPipeline.")
        return

    # Convert to list-of-dicts for easy scanning
    df = list(rows)

    phrase = "final in-season estimate"

    # Find all rows that contain the FISE phrase
    fise_indices = [
        i for i, r in enumerate(df)
        if isinstance(r["text_line"], str)
        and phrase in r["text_line"].lower()
    ]

    print(f"🔎 Found {len(fise_indices)} 'Final in-season estimate' rows")

    rows_to_delete = set()
    count_rows_removed = set()

    # Process each FISE row
    for idx in fise_indices:
        row = df[idx]
        rows_to_delete.add(row["id"])  # always delete the FISE row

        text = row["text_line"]
        pdf = row["pdf_name"]
        page = row["page_num"]

        # Case 1 — FISE row also contains count pattern
        if has_count_sequence(text):
            count_rows_removed.add(row["id"])
            continue

        # Case 2 — look up to 5 previous rows (same pdf_name & page_num)
        for offset in range(1, 6):
            prev_index = idx - offset
            if prev_index < 0:
                break

            prev = df[prev_index]
            if prev["pdf_name"] != pdf:
                continue
            if prev["page_num"] != page:
                continue

            if has_count_sequence(prev["text_line"]):
                rows_to_delete.add(prev["id"])
                count_rows_removed.add(prev["id"])
                break

    # ------------------------------------------------------------
    # Apply deletions to SQLite
    # ------------------------------------------------------------
    print(f"🗑️ Removing {len(rows_to_delete)} rows...")

    if rows_to_delete:
        with get_conn() as conn:
            conn.executemany(
                "DELETE FROM Escapement_PlotPipeline WHERE id = ?",
                [(rid,) for rid in rows_to_delete]
            )
            conn.commit()

    print("📊 Removal summary:")
    print(f"   • Total rows removed: {len(rows_to_delete)}")
    print(f"   • 'Final in-season estimate' rows: {len(fise_indices)}")
    print(f"   • Associated count rows removed: {len(count_rows_removed)}")

    print("✅ Step 6 complete.")


if __name__ == "__main__":
    main()