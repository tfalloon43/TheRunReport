"""
step27_columnreorg.py
------------------------------------------------------------
Reorganize columns in Escapement_PlotPipeline into a clean,
final schema and update the table IN PLACE.

This version does NOT create Escapement_Final.
"""

import sqlite3
from pathlib import Path
import pandas as pd

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

print("🏗️ Step 27: Reorganizing columns in Escapement_PlotPipeline…")
print(f"🗄️ Using DB → {DB_PATH}")

# ------------------------------------------------------------
# DB Helper
# ------------------------------------------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    # Load data
    with get_conn() as conn:
        df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

    print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

    ordered_columns = [
        "pdf_name",
        "facility",
        "basin",
        "species",
        "Family",
        "Stock_BO",
        "Stock",
        "date_iso",
        "Adult Total",
        "Jack Total",
        "Total Eggtake",
        "On Hand Adults",
        "On Hand Jacks",
        "Lethal Spawned",
        "Live Spawned",
        "Released",
        "Live Shipped",
        "Mortality",
        "Surplus",
    ]

    rename_map = {
        "Adult_Total": "Adult Total",
        "Jack_Total": "Jack Total",
        "Total_Eggtake": "Total Eggtake",
        "On_Hand_Adults": "On Hand Adults",
        "On_Hand_Jacks": "On Hand Jacks",
        "Lethal_Spawned": "Lethal Spawned",
        "Live_Spawned": "Live Spawned",
        "Released": "Released",
        "Live_Shipped": "Live Shipped",
        "Mortality": "Mortality",
        "Surplus": "Surplus",
    }

    df = df.rename(columns=rename_map)

    drop_columns = [
        "id",
        "page_num",
        "text_line",
        "date",
        "stock_presence",
        "stock_presence_lower",
        "Hatchery_Name",
        "TL2",
        "TL3",
        "count_data",
        "TL4",
        "TL5",
        "TL6",
    ]

    df = df.drop(columns=[c for c in drop_columns if c in df.columns], errors="ignore")

    existing_cols = [c for c in ordered_columns if c in df.columns]
    df_final = df[existing_cols]

    print(f"📊 Final cleaned dataset shape: {df_final.shape[0]:,} rows × {df_final.shape[1]} columns")

    with get_conn() as conn:
        df_final.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)

    print("💾 Updated table → Escapement_PlotPipeline (in place)")
    print("✅ Step 27 complete — table cleaned and reorganized.")


if __name__ == "__main__":
    main()
