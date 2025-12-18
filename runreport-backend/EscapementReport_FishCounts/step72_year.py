# step72_year.py
# ------------------------------------------------------------
# Step 72: Add year column
#
# Adds a `year` column to Escapement_PlotPipeline by extracting
# the year from `date_iso`.
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("📅 Step 72: Creating `year` column from date_iso...")

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
# VALIDATE REQUIRED COLUMNS
# ------------------------------------------------------------
if "date_iso" not in df.columns:
    raise ValueError("❌ Missing required column 'date_iso' in Escapement_PlotPipeline.")

# ------------------------------------------------------------
# BUILD YEAR COLUMN
# ------------------------------------------------------------
dt = pd.to_datetime(df["date_iso"], errors="coerce")
df["year"] = dt.dt.year.astype("Int64")

missing_years = int(df["year"].isna().sum())
if missing_years:
    print(f"⚠️ {missing_years:,} row(s) have invalid date_iso; `year` set to NULL for those.")

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 72 complete — `year` column added.")
