"""
step23_counts.py
------------------------------------------------------------
Expand count_data into 11 numeric columns inside Escapement_PlotPipeline.

Rules:
    • Only rows with a non-empty date get expanded.
    • Exactly 11 numeric columns:
          1) Adult Total
          2) Jack Total
          3) Total Eggtake
          4) On Hand Adults
          5) On Hand Jacks
          6) Lethal Spawned
          7) Live Spawned
          8) Released
          9) Live Shipped
         10) Mortality
         11) Surplus
    • Fewer than 11 → pad with NULL
    • More than 11 → all NULL
    • Dash/missing/invalid → NULL
    • Store integers only (no decimals)

Updates table:
    Escapement_PlotPipeline
"""

import sqlite3
import re
import numpy as np
from pathlib import Path

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

print(f"🗄️ Using DB → {DB_PATH}")

# ------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ------------------------------------------------------------
# Columns to add
# ------------------------------------------------------------
COUNT_COLS = [
    "Adult_Total",
    "Jack_Total",
    "Total_Eggtake",
    "On_Hand_Adults",
    "On_Hand_Jacks",
    "Lethal_Spawned",
    "Live_Spawned",
    "Released",
    "Live_Shipped",
    "Mortality",
    "Surplus",
]


def ensure_columns():
    """Add missing count columns to Escapement_PlotPipeline."""
    with get_conn() as conn:
        for col in COUNT_COLS:
            try:
                conn.execute(f"ALTER TABLE Escapement_PlotPipeline ADD COLUMN {col} TEXT")
                print(f"   ✔ Added column: {col}")
            except sqlite3.OperationalError:
                # column already exists
                pass
        conn.commit()


# ------------------------------------------------------------
# Parse count_data into 11 whole-number slots
# ------------------------------------------------------------
def parse_count_row(date_val, count_data_val):
    """Return a list of 11 values (string or None)."""

    # No date → all blank
    if not date_val or str(date_val).strip() == "":
        return [None] * 11

    if not count_data_val or not str(count_data_val).strip():
        return [None] * 11

    tokens = re.split(r"\s+", str(count_data_val).strip())
    clean_vals = []

    for t in tokens:
        t = t.replace(",", "")
        if t in ("", "-", "--"):
            clean_vals.append(None)
        else:
            try:
                clean_vals.append(str(int(t)))  # whole number only
            except Exception:
                clean_vals.append(None)

    # Adjust length
    if len(clean_vals) < 11:
        clean_vals += [None] * (11 - len(clean_vals))
    elif len(clean_vals) > 11:
        return [None] * 11

    return clean_vals


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    print("🏗️ Step 23: Expanding count_data into 11 numeric columns...")

    ensure_columns()

    # Load data
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, date, count_data
            FROM Escapement_PlotPipeline
            ORDER BY id
        """).fetchall()

    print(f"📄 Loaded {len(rows):,} rows.")

    updates = []
    populated = 0

    for r in rows:
        row_id = r["id"]
        date_val = r["date"]
        count_val = r["count_data"]

        parsed = parse_count_row(date_val, count_val)
        if any(parsed):
            populated += 1

        updates.append((parsed, row_id))

    # Write back to DB
    with get_conn() as conn:
        for parsed, row_id in updates:
            cols_sql = ", ".join(f"{col} = ?" for col in COUNT_COLS)
            sql = f"UPDATE Escapement_PlotPipeline SET {cols_sql} WHERE id = ?"

            conn.execute(
                sql,
                (*parsed, row_id)
            )

        conn.commit()

    print(f"🎉 Step 23 complete!")
    print(f"📊 {populated:,} rows populated with expanded count data")
    print("🔄 Escapement_PlotPipeline updated with 11 count columns")


if __name__ == "__main__":
    main()