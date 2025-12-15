# step64_remove_old.py
# ------------------------------------------------------------
# Step 64: Retain only current year + prior 10 years
#
# Keeps rows in Escapement_PlotPipeline where date_iso falls
# within [current_year - 10, current_year]. Rows outside that
# rolling window are removed.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 64: Trimming Escapement_PlotPipeline to current year + prior 10 years...")

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
# FILTER TO ROLLING WINDOW
# ------------------------------------------------------------
if "date_iso" not in df.columns:
    raise ValueError("❌ Missing required column 'date_iso' in Escapement_PlotPipeline.")

df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")

current_year = pd.Timestamp.today().year
min_year = current_year - 10

keep_mask = df["date_iso"].dt.year.between(min_year, current_year, inclusive="both")
removed = int((~keep_mask).sum())

df_filtered = df.loc[keep_mask].reset_index(drop=True)

print(f"🗑️ Rows removed outside {min_year}–{current_year}: {removed:,}")
print(f"📊 Remaining rows: {len(df_filtered):,}")

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df_filtered.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 64 complete — retained current year + prior 10 years.")
