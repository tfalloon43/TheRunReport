"""
step17_StockBO.py
-----------------------------------------
Creates Stock_BO column inside Escapement_PlotPipeline table.

Logic matches old 12_Stock_BO.py:

Phase 1 → Stock_BO = TL4 for rows with a date
Phase 2 → For rows where stock_presence_lower exists,
          append that row's TL6 to the *previous* row.

Finally cleans nan/NaN/None artifacts.
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
# Ensure Stock_BO column exists
# ------------------------------------------------------------

def ensure_stockbo_column():
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE Escapement_PlotPipeline ADD COLUMN Stock_BO TEXT")
            conn.commit()
            print("   ✔ Added Stock_BO column")
        except sqlite3.OperationalError:
            print("   ℹ️ Stock_BO column already exists")

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

def has_valid_date(date_val: str) -> bool:
    if not isinstance(date_val, str):
        return False
    date_val = date_val.strip()
    if not date_val or date_val.lower() in ("nan", "none"):
        return False
    return True

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    print("🏗️ Step 17: Building Stock_BO...")

    ensure_stockbo_column()

    # Load everything
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, date, TL4, TL6, stock_presence_lower, Stock_BO
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"🔎 Loaded {len(rows):,} rows")

    # Convert rows to a mutable dict list
    data = [dict(r) for r in rows]

    # ------------------------------------------------------------
    # Phase 1 — copy TL4 where date exists
    # ------------------------------------------------------------
    initialized = 0
    for r in data:
        if has_valid_date(r.get("date", "")):
            r["Stock_BO"] = (r.get("TL4") or "").strip()
            initialized += 1
        else:
            r["Stock_BO"] = ""

    print(f"✅ Phase 1: Initialized {initialized} rows from TL4")

    # ------------------------------------------------------------
    # Phase 2 — append TL6 to previous row when stock_presence_lower exists
    # ------------------------------------------------------------
    appended = 0
    for i in range(1, len(data)):
        row = data[i]
        spl = (row.get("stock_presence_lower") or "").strip()
        tl6 = (row.get("TL6") or "").strip()

        if spl and tl6 and tl6.lower() not in ("nan", "none"):
            prev = data[i - 1]
            current_val = (prev.get("Stock_BO") or "").strip()
            new_val = f"{current_val} {tl6}".strip()
            new_val = " ".join(new_val.split())  # normalize spaces
            # Remove standalone nan/None tokens only (preserve words like McKernan)
            new_val = re.sub(r"\b(?:nan|NaN|None)\b", "", new_val).strip()
            prev["Stock_BO"] = new_val
            appended += 1

    print(f"✅ Phase 2: Appended TL6 into previous rows {appended:,} times")

    # ------------------------------------------------------------
    # Final cleanup
    # ------------------------------------------------------------
    for r in data:
        cleaned = (r.get("Stock_BO") or "")
        cleaned = re.sub(r"\b(?:nan|NaN|None)\b", "", cleaned)
        cleaned = " ".join(cleaned.split()).strip()
        r["Stock_BO"] = cleaned

    # ------------------------------------------------------------
    # Write results back into DB
    # ------------------------------------------------------------
    with get_conn() as conn:
        conn.executemany(
            "UPDATE Escapement_PlotPipeline SET Stock_BO = ? WHERE id = ?",
            [(r["Stock_BO"], r["id"]) for r in data]
        )
        conn.commit()

    populated = sum(1 for r in data if r["Stock_BO"])

    print("\n🎉 Step 17 complete!")
    print(f"📊 {populated:,} of {len(data):,} rows have Stock_BO")
    print("🔄 Escapement_PlotPipeline updated")

if __name__ == "__main__":
    main()
