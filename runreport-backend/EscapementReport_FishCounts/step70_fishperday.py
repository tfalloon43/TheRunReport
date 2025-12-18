# step70_fishperday.py
# ------------------------------------------------------------
# Step 70: Compute fish per day
#
# Adds a fishperday column = adult_diff_plot / day_diff_plot,
# rounded to the nearest hundredth.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🐟 Step 70: Calculating fishperday (adult_diff_plot / day_diff_plot)...")

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
# VALIDATE COLUMNS
# ------------------------------------------------------------
required = ["adult_diff_plot", "day_diff_plot"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# COMPUTE fishperday
# ------------------------------------------------------------
df["adult_diff_plot"] = pd.to_numeric(df["adult_diff_plot"], errors="coerce")
df["day_diff_plot"] = pd.to_numeric(df["day_diff_plot"], errors="coerce")

with pd.option_context("mode.use_inf_as_na", True):
    df["fishperday"] = (df["adult_diff_plot"] / df["day_diff_plot"]).round(2)

removed_div0 = df["fishperday"].isna().sum()
if removed_div0:
    print(f"ℹ️ fishperday could not be computed for {removed_div0:,} rows (NaN/inf from division). Values left as NaN.")

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 70 complete — fishperday column added.")
