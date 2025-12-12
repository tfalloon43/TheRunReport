"""
step28_duplicates_delete.py
------------------------------------------------------------
Remove duplicate rows from Escapement_PlotPipeline.

Rules:
    • Ignore pdf_name and date_iso when detecting duplicates
    • Keep the FIRST occurrence of each unique row
    • Write the deduped table back into Escapement_PlotPipeline (replace)
"""

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 28: Removing duplicates from Escapement_PlotPipeline...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent                      # runreport-backend/
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

print(f"🗄️ Using DB → {DB_PATH}")

# ------------------------------------------------------------
# DB helper
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def main():
    with get_conn() as conn:
        df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

    initial_count = len(df)
    print(f"📥 Loaded {initial_count:,} rows")

    ignore_cols = {"pdf_name", "date_iso"}
    cols_to_check = [c for c in df.columns if c not in ignore_cols]

    print(f"🔎 Checking duplicates on {len(cols_to_check)} columns:")
    print("   " + ", ".join(cols_to_check))

    df_deduped = df.drop_duplicates(subset=cols_to_check, keep="first")
    removed = initial_count - len(df_deduped)

    print(f"🧹 Removed {removed:,} duplicate rows")
    print(f"📊 Final row count: {len(df_deduped):,}")

    with get_conn() as conn:
        df_deduped.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)

    print("💾 Updated Escapement_PlotPipeline in place.")
    print("✅ Step 28 complete!")


if __name__ == "__main__":
    main()
    