# step49_column_reorg.py
# ------------------------------------------------------------
# Step 49 (Final Column Reorg): Trim DB table to final plot-ready columns.
#
# This script:
#   • Loads Escapement_PlotPipeline from the DB
#   • Removes *all* intermediate iteration columns
#   • Keeps ONLY the final clean, plot-ready fields
#   • Writes the trimmed dataset back into the DB
#
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 49: Preparing final plot-ready dataset inside DB...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD DATA FROM DB
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

before_cols = len(df.columns)
print(f"✅ Loaded {len(df):,} rows and {before_cols} columns from Escapement_PlotPipeline")

# ------------------------------------------------------------
# Define FINAL column order (only the clean plot-ready fields)
# ------------------------------------------------------------
final_columns = [
    "index",
    "pdf_name",
    "facility",
    "basin",
    "species",
    "Family",
    "Stock_BO",
    "Stock",
    "date_iso",
    "Adult Total",          # DB field equivalent of Adult_Total
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

# ------------------------------------------------------------
# Validate presence
# ------------------------------------------------------------
missing_cols = [c for c in final_columns if c not in df.columns]
if missing_cols:
    print(f"⚠️ Warning: Missing expected final columns: {missing_cols}")

# keep only those that exist
keep_cols = [c for c in final_columns if c in df.columns]

df_final = df[keep_cols].copy()
after_cols = len(df_final.columns)

# ------------------------------------------------------------
# WRITE TRIMMED DATA BACK TO DB
# ------------------------------------------------------------
print("💾 Writing trimmed final dataset back to Escapement_PlotPipeline...")

df_final.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
dropped_cols = before_cols - after_cols
print("✅ Step 49 (Column Reorg) Complete!")
print(f"🧾 Columns kept: {after_cols}")
print(f"🗑️ Columns dropped: {dropped_cols}")
print(f"📊 Final columns: {', '.join(df_final.columns)}")
print(f"🔢 Total rows: {len(df_final):,}")