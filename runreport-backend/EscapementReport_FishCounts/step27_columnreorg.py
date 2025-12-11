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
        "index",
        "pdf_name",
        "facility",
        "basin",
        "species",
        "Family",
        "Stock_BO",
        "Stock",
        "date_iso",
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
        "pdf_date",
        "day_diff_plot",
        "adult_diff_plot",
        "Biological_Year",
        "Biological_Year_Length",
    ]

    # Normalize any space-delimited names back to underscore schema
    rename_map = {
        "Adult Total": "Adult_Total",
        "Jack Total": "Jack_Total",
        "Total Eggtake": "Total_Eggtake",
        "On Hand Adults": "On_Hand_Adults",
        "On Hand Jacks": "On_Hand_Jacks",
        "Lethal Spawned": "Lethal_Spawned",
        "Live Spawned": "Live_Spawned",
        "Live Shipped": "Live_Shipped",
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
