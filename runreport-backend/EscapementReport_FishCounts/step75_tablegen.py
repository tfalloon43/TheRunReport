# step75_tablegen.py
# ------------------------------------------------------------
# Step 75: Generate a 366-day template table for basinfamily
# 
# Builds an empty daily-counts template with:
#   - Rows: Every MM-DD in a year (including 12-31)
#   - Columns: Unique basinfamily values from Escapement_PlotPipeline
# Saves to local.db as EscapementReports_dailycounts.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

print("🏗️ Step 75: Generating EscapementReports_dailycounts (basinfamily template)...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)
print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
# VALIDATE REQUIRED COLUMN
# ------------------------------------------------------------
if "basinfamily" not in df.columns:
    raise ValueError("❌ Missing required column 'basinfamily' in Escapement_PlotPipeline.")

df["basinfamily"] = df["basinfamily"].astype(str).str.strip()

uniques = sorted(df["basinfamily"].dropna().unique())
print(f"📊 Found {len(uniques)} unique basinfamily values")

# ------------------------------------------------------------
# CREATE BASE DATE FRAME (3 years x 366 days incl 12-31) + metric_type column
# ------------------------------------------------------------
dates = [(datetime(2024, 1, 1) + timedelta(days=i)).strftime("%m-%d") for i in range(366)]

frames = []
metric_labels = ["current_year", "previous_year", "10_year"]

for label in metric_labels:
    frames.append(pd.DataFrame({"metric_type": label, "MM-DD": dates}))

base_df = pd.concat(frames, ignore_index=True)

# ------------------------------------------------------------
# BUILD TEMPLATE TABLE
# ------------------------------------------------------------
if uniques:
    zero_matrix = pd.DataFrame(np.zeros((len(base_df), len(uniques))), columns=uniques)
    table = pd.concat([base_df, zero_matrix], axis=1)
else:
    table = base_df.copy()
    print("ℹ️ No basinfamily values found; table will only contain metric_type and MM-DD.")

# ------------------------------------------------------------
# WRITE TO DB
# ------------------------------------------------------------
table.to_sql("EscapementReports_dailycounts", conn, if_exists="replace", index=False)
conn.close()

print(f"✅ Step 75 complete — EscapementReports_dailycounts created with {len(table)} rows and {len(table.columns)} columns.")
