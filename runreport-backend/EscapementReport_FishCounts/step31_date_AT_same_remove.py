"""
step31_date_AT_same_remove.py
-----------------------------------------
Within each biological identity (facility, species, Stock, Stock_BO),
if multiple rows share the same date_iso and Adult_Total, keep only
the row with the earliest pdf_date.

Writes the deduped result back into Escapement_PlotPipeline.
"""

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 31: Removing same date_iso + Adult_Total duplicates using earliest pdf_date...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
DB_PATH = BACKEND_ROOT / "0_db" / "local.db"

print(f"🗄️ Using DB → {DB_PATH}")


def main():
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

    if "date_iso" not in df.columns:
        raise ValueError("❌ Missing required column 'date_iso'. Run date normalization first.")
    if "pdf_date" not in df.columns:
        raise ValueError("❌ Missing required column 'pdf_date'. Run pdf_date extraction first.")

    key_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total"]

    # Ensure key columns exist
    for col in key_cols:
        if col not in df.columns:
            df[col] = ""

    # Parse dates for ordering (keep originals in place)
    df["_date_iso_dt"] = pd.to_datetime(df["date_iso"], errors="coerce")
    df["_pdf_date_dt"] = pd.to_datetime(df["pdf_date"], errors="coerce")

    # Stable sort so earliest pdf_date within each key group is first; tie-break by original order
    df["_orig_order"] = range(len(df))
    df_sorted = df.sort_values(
        by=key_cols + ["_pdf_date_dt", "_orig_order"],
        ascending=[True, True, True, True, True, True, True, True],
        na_position="last",
        kind="mergesort",
    )

    # Drop duplicates keeping the earliest pdf_date (first after sort)
    df_deduped = df_sorted.drop_duplicates(subset=key_cols, keep="first")

    # Clean helper columns
    df_deduped = df_deduped.drop(columns=["_date_iso_dt", "_pdf_date_dt", "_orig_order"])

    removed = len(df) - len(df_deduped)
    print(f"🧹 Removed {removed:,} duplicate rows based on (facility, species, Stock, Stock_BO, date_iso, Adult_Total) keeping earliest pdf_date.")
    print(f"📊 Final row count: {len(df_deduped):,}")

    with sqlite3.connect(DB_PATH) as conn:
        df_deduped.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)

    print("✅ Step 31 complete — Escapement_PlotPipeline updated.")


if __name__ == "__main__":
    main()
