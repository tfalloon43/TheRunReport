# step62_remove_MC.py
# ------------------------------------------------------------
# Step 62: Remove M/C Stock rows
#
# Removes any rows in Escapement_PlotPipeline where the Stock
# column equals "M" or "C".
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 62: Removing Stock 'M' and 'C' rows from Escapement_PlotPipeline...")

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
# FILTER OUT M/C STOCK ROWS
# ------------------------------------------------------------
if "Stock" not in df.columns:
    raise ValueError("❌ Missing required column 'Stock' in Escapement_PlotPipeline.")

mask_mc = df["Stock"].isin(["M", "C"])
removed = int(mask_mc.sum())

df_filtered = df.loc[~mask_mc].reset_index(drop=True)

print(f"🗑️ Rows removed (Stock == 'M' or 'C'): {removed:,}")
print(f"📊 Remaining rows: {len(df_filtered):,}")

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df_filtered.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 62 complete — Stock 'M' and 'C' rows removed.")
