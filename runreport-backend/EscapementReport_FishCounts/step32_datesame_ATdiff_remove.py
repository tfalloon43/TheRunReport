"""
step32_datesame_ATdiff_remove.py
-----------------------------------------
Within each biological identity (facility, species, Stock, Stock_BO),
if multiple rows share the same date_iso, keep only the row with the
largest Adult_Total. Writes the deduped result back into
Escapement_PlotPipeline.
"""

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 32: Removing same date_iso (keep largest Adult_Total)...")

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

    required_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    # Normalize Adult_Total to numeric for comparison; keep original values alongside if needed
    df["_Adult_Total_num"] = pd.to_numeric(df["Adult_Total"], errors="coerce")
    df["_orig_order"] = range(len(df))

    # Sort so largest Adult_Total comes first per key/date, then original order
    key_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso"]
    df_sorted = df.sort_values(
        by=key_cols + ["_Adult_Total_num", "_orig_order"],
        ascending=[True, True, True, True, True, False, True],
        na_position="last",
        kind="mergesort",
    )

    # Drop duplicates on key_cols keeping the row with largest Adult_Total (first after sort)
    df_deduped = df_sorted.drop_duplicates(subset=key_cols, keep="first")

    # Clean helper columns
    df_deduped = df_deduped.drop(columns=["_Adult_Total_num", "_orig_order"])

    removed = len(df) - len(df_deduped)
    print(f"🧹 Removed {removed:,} rows where date_iso matched within the same biological identity, keeping largest Adult_Total.")
    print(f"📊 Final row count: {len(df_deduped):,}")

    with sqlite3.connect(DB_PATH) as conn:
        df_deduped.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)

    print("✅ Step 32 complete — Escapement_PlotPipeline updated.")


if __name__ == "__main__":
    main()
