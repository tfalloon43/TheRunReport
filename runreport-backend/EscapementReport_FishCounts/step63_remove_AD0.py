# step63_remove_AD0.py
# ------------------------------------------------------------
# Step 63: Remove rows with adult_diff_plot == 0
#
# Loads Escapement_PlotPipeline and drops any rows where
# adult_diff_plot equals zero.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🧹 Step 63: Removing rows with adult_diff_plot == 0...")

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
# FILTER OUT ZERO adult_diff_plot ROWS (excluding current year)
# ------------------------------------------------------------
if "adult_diff_plot" not in df.columns:
    raise ValueError("❌ Missing required column 'adult_diff_plot' in Escapement_PlotPipeline.")
if "date_iso" not in df.columns:
    raise ValueError("❌ Missing required column 'date_iso' in Escapement_PlotPipeline.")

# Coerce to numeric in case of string types
df["adult_diff_plot"] = pd.to_numeric(df["adult_diff_plot"], errors="coerce")
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")

current_year = pd.Timestamp.today().year
mask_current_year = df["date_iso"].dt.year == current_year

# Remove zero diffs only for rows NOT in the current year
mask_zero = (df["adult_diff_plot"] == 0) & (~mask_current_year)
removed = int(mask_zero.sum())

df_filtered = df.loc[~mask_zero].reset_index(drop=True)

print(f"🗑️ Rows removed (adult_diff_plot == 0): {removed:,}")
print(f"📊 Remaining rows: {len(df_filtered):,}")

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df_filtered.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 63 complete — zero adult_diff_plot rows removed.")
