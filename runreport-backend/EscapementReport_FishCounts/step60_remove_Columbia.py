# step60_remove_Columbia.py
# ------------------------------------------------------------
# Step 60: Remove Columbia River rows
#
# Removes any rows in Escapement_PlotPipeline where the basin
# column contains "Columbia River" (case-insensitive).
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 60: Removing Columbia River rows from Escapement_PlotPipeline...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD TABLE
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)
print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
# FILTER OUT COLUMBIA RIVER BASIN ROWS
# ------------------------------------------------------------
if "basin" not in df.columns:
    raise ValueError("❌ Missing required column 'basin' in Escapement_PlotPipeline.")

mask_columbia = df["basin"].str.contains("Columbia River", case=False, na=False)
removed = int(mask_columbia.sum())

df_filtered = df.loc[~mask_columbia].reset_index(drop=True)

print(f"🗑️ Rows removed (basin contains 'Columbia River'): {removed:,}")
print(f"📊 Remaining rows: {len(df_filtered):,}")

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df_filtered.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 60 complete — Columbia River rows removed.")
