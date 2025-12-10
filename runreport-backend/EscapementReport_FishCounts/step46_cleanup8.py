# step46_cleanup8.py
# ------------------------------------------------------------
# Step 46 (v8 Cleanup):
# Remove weak short-run clusters where:
#       x_count8 ∈ [1, 2, 3]
# EXCEPT keep entries from the current year.
#
# Operates entirely inside the database.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

print("🧹 Step 46 (v8 Cleanup): Removing weak short-run clusters (x_count8 = 1–3)...")

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
# VALIDATE REQUIRED COLUMNS
# ------------------------------------------------------------
required_cols = ["date_iso", "x_count8"]
missing = [c for c in required_cols if c not in df.columns]

if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["x_count8"] = pd.to_numeric(df["x_count8"], errors="coerce").fillna(0).astype(int)

# ------------------------------------------------------------
# CLEANUP LOGIC
# ------------------------------------------------------------
current_year = datetime.now().year
before = len(df)

# Keep rows where:
#   - x_count8 NOT in [1,2,3] OR
#   - the row is from the current year
mask_keep = (~df["x_count8"].isin([1, 2, 3])) | (df["date_iso"].dt.year == current_year)

df = df[mask_keep].reset_index(drop=True)
removed = before - len(df)

# ------------------------------------------------------------
# SAVE RESULTS BACK TO DB
# ------------------------------------------------------------
print("💾 Writing cleaned table back to database...")
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Cleanup 8 Complete!")
print(f"🧽 Removed {removed:,} rows where x_count8 ∈ [1, 2, 3] (excluding {current_year}).")
print(f"📊 Final dataset: {len(df):,} rows remain.")