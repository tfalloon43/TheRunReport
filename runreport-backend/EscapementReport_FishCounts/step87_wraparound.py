# step87_wraparound.py
# ------------------------------------------------------------
# Step 87: Add 01-01 wraparound rows for each biological indicator
#
# Biological indicator = (river, Species_Plot)
#
# For each indicator, ensure a row exists for MM-DD = 01-01, with:
#   current_year(01-01)   = previous_year(12-31)
#   10_year(01-01)        = 10_year(12-31)
#   previous_year(01-01)  = (10_year(12-31) + previous_year(01-07)) / 2
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🧭 Step 87: Adding 01-01 wraparound rows to EscapementReport_PlotData...")

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
df = pd.read_sql_query("SELECT * FROM EscapementReport_PlotData;", conn)
print(f"📂 Loaded {len(df):,} rows from EscapementReport_PlotData")

column_order = list(df.columns)

# ------------------------------------------------------------
# VALIDATE REQUIRED COLUMNS
# ------------------------------------------------------------
required_cols = ["river", "Species_Plot", "MM-DD", "current_year", "previous_year", "10_year"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
for col in ["river", "Species_Plot", "MM-DD"]:
    df[col] = df[col].astype(str).str.strip()

metric_cols = ["current_year", "previous_year", "10_year"]
df[metric_cols] = df[metric_cols].apply(pd.to_numeric, errors="coerce")

if "id" in df.columns:
    df = df.drop(columns=["id"])

# ------------------------------------------------------------
# BUILD WRAPAROUND ROWS
# ------------------------------------------------------------
keys = ["river", "Species_Plot"]
base_keys = df[keys].drop_duplicates()

val_1231 = (
    df[df["MM-DD"] == "12-31"][keys + ["previous_year", "10_year"]]
    .rename(columns={"previous_year": "prev_1231", "10_year": "ten_1231"})
)
val_0107 = (
    df[df["MM-DD"] == "01-07"][keys + ["previous_year"]]
    .rename(columns={"previous_year": "prev_0107"})
)

wrap_rows = base_keys.merge(val_1231, on=keys, how="left").merge(
    val_0107, on=keys, how="left"
)

wrap_rows["MM-DD"] = "01-01"
wrap_rows["current_year"] = wrap_rows["prev_1231"]
wrap_rows["10_year"] = wrap_rows["ten_1231"]
wrap_rows["previous_year"] = (wrap_rows["ten_1231"] + wrap_rows["prev_0107"]) / 2

# Add any additional columns expected downstream
for col in column_order:
    if col not in wrap_rows.columns and col != "id":
        wrap_rows[col] = pd.NA

wrap_rows = wrap_rows[[c for c in column_order if c != "id"]]

# Drop existing 01-01 rows and replace with computed ones
df = df[df["MM-DD"] != "01-01"]
df_final = pd.concat([df, wrap_rows], ignore_index=True)

# ------------------------------------------------------------
# SORT + ID
# ------------------------------------------------------------
df_final["date_obj"] = pd.to_datetime("2024-" + df_final["MM-DD"], errors="coerce")
sort_cols = [c for c in ["river", "Species_Plot", "date_obj"] if c in df_final.columns]
df_final = df_final.sort_values(sort_cols).drop(columns=["date_obj"]).reset_index(drop=True)
df_final.insert(0, "id", range(1, len(df_final) + 1))

# ------------------------------------------------------------
# ROUND CURRENT/PREVIOUS YEARS (KEEP 10_YEAR DECIMALS)
# ------------------------------------------------------------
for col in ["current_year", "previous_year"]:
    if col in df_final.columns:
        df_final[col] = pd.to_numeric(df_final[col], errors="coerce").round(0)

# ------------------------------------------------------------
# WRITE BACK
# ------------------------------------------------------------
df_final.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Step 87 complete — 01-01 wraparound rows added/updated.")
print(f"📊 Rows: {len(df_final):,}")
