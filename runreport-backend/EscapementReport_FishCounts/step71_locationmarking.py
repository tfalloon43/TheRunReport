# step71_locationmarking.py
# ------------------------------------------------------------
# Step 71: Add basinfamily identifier
#
# For every row in Escapement_PlotPipeline, create:
#   basinfamily = basin + " - " + Family
# No other identifier columns are added.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 71: Creating basinfamily identifiers for all rows...")

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
required = ["basin", "Family"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# NORMALIZE AND BUILD basinfamily
# ------------------------------------------------------------
df["basin"] = df["basin"].astype(str).str.strip()
df["Family"] = df["Family"].astype(str).str.strip()

df["basinfamily"] = df["basin"] + " - " + df["Family"]

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 71 complete — basinfamily column added.")
