# step74_count_days.py
# ------------------------------------------------------------
# Step 74: Expand date_iso into Day1 → DayN columns based on day_diff_plot
#
# For each row:
#   - Treat date_iso as the end date
#   - Count backwards by day_diff_plot days
#   - Fill Day1, Day2, ... with the corresponding MM-DD values
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from datetime import timedelta
from pathlib import Path

print("🏗️ Step 74: Expanding date_iso into Day1 → DayN columns using day_diff_plot...")

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
required_cols = ["date_iso", "day_diff_plot"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["day_diff_plot"] = pd.to_numeric(df["day_diff_plot"], errors="coerce").fillna(0).astype(int)

# ------------------------------------------------------------
# PREPARE DAY COLUMNS
# ------------------------------------------------------------
max_days = int(df["day_diff_plot"].max())
if max_days <= 0:
    print("ℹ️ No positive day_diff_plot values found; skipping Day column creation.")
    df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
    conn.close()
    print("✅ Step 74 complete (no Day columns added).")
    exit()

print(f"📅 Maximum day_diff_plot = {max_days} → creating Day1 through Day{max_days}")

day_cols = [f"Day{i+1}" for i in range(max_days)]
for col in day_cols:
    df[col] = ""

# ------------------------------------------------------------
# FILL DAY COLUMNS
# ------------------------------------------------------------
for idx, row in df.iterrows():
    end_date = row["date_iso"]
    days_back = row["day_diff_plot"]

    if pd.isna(end_date) or days_back <= 0:
        continue

    for i in range(days_back):
        day_label = f"Day{i+1}"
        if day_label in df.columns:
            df.at[idx, day_label] = (end_date - timedelta(days=i)).strftime("%m-%d")

# ------------------------------------------------------------
# WRITE BACK TO DATABASE
# ------------------------------------------------------------
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

print(f"✅ Step 74 complete — created Day1 → Day{max_days} columns.")
