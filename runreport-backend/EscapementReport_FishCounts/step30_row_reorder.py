"""
step30_row_reorder.py
-----------------------------------------
Reorder rows in Escapement_PlotPipeline using the following hierarchy:

1️⃣ facility        (A → Z)
2️⃣ species         (A → Z)
3️⃣ Stock           (A → Z)
4️⃣ Stock_BO        (A → Z)
5️⃣ pdf_date        (oldest → newest)
6️⃣ Adult_Total     (highest → lowest)

Writes table back to SQLite (local.db) in sorted order.
"""

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 30: Reordering Escapement_PlotPipeline...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent              # runreport-backend/
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

print(f"🗄️ Using DB: {DB_PATH}")

def main():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline", conn)

    print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

    if "pdf_date" not in df.columns:
        raise ValueError("❌ Missing required column 'pdf_date'. Run step29_pdf_date.py first.")

    df["pdf_date"] = pd.to_datetime(df["pdf_date"], errors="coerce")

    if "Adult_Total" in df.columns:
        df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
    else:
        print("⚠️ No 'Adult_Total' column found — sorting by adult count will be skipped.")

    sort_columns = ["facility", "species", "Stock", "Stock_BO", "pdf_date", "Adult_Total"]
    ascending_order = [True, True, True, True, True, False]  # Adult_Total is descending

    existing_sort_columns = [c for c in sort_columns if c in df.columns]
    existing_ascending = [ascending_order[sort_columns.index(c)] for c in existing_sort_columns]

    df_sorted = df.sort_values(
        by=existing_sort_columns,
        ascending=existing_ascending,
        na_position="last"
    )

    print("🔄 Applying sort order:")
    for col, asc in zip(existing_sort_columns, existing_ascending):
        print(f"   • {col} ({'ASC' if asc else 'DESC'})")

    df_sorted.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
    conn.close()

    print("✅ Reordering complete")
    print(f"📊 Final row count: {len(df_sorted):,}")
    print("🎯 Rows grouped + ordered by facility → species → Stock → Stock_BO → pdf_date → Adult_Total (desc)")
    print("🔄 Escapement_PlotPipeline updated in local.db")


if __name__ == "__main__":
    main()
