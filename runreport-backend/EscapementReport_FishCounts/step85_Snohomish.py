# step85_Snohomish.py
# ------------------------------------------------------------
# Step 85: Aggregate Skykomish + Snoqualmie into Snohomish River rows
#
# Logic:
#   - Keep all existing rows.
#   - For rows where river ∈ {Skykomish River, Snoqualmie River},
#     group by MM-DD and Species_Plot and sum metric columns
#     (current_year, previous_year, 10_year).
#   - Create new rows with:
#       river = "Snohomish River"
#       identifier = f"Snohomish River - {Species_Plot}"
#   - Append to EscapementReport_PlotData in local.db.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🌊 Step 85: Aggregating Skykomish + Snoqualmie into Snohomish River rows...")

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

# Preserve column order to keep consistency
column_order = list(df.columns)

# ------------------------------------------------------------
# VALIDATE REQUIRED COLUMNS
# ------------------------------------------------------------
required = ["MM-DD", "Species_Plot", "river", "identifier", "current_year", "previous_year", "10_year"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
metric_cols = ["current_year", "previous_year", "10_year"]
df[metric_cols] = df[metric_cols].apply(pd.to_numeric, errors="coerce")
df["MM-DD"] = df["MM-DD"].astype(str).str.strip()
df["Species_Plot"] = df["Species_Plot"].astype(str).str.strip()
df["river"] = df["river"].astype(str).str.strip()

# ------------------------------------------------------------
# FILTER TRIBUTARIES AND AGGREGATE
# ------------------------------------------------------------
trib_rivers = ["Skykomish River", "Snoqualmie River"]
df_sub = df[df["river"].isin(trib_rivers)].copy()

print(f"📊 Found {len(df_sub):,} tributary rows to aggregate")

group_cols = ["MM-DD", "Species_Plot"]
agg_df = df_sub.groupby(group_cols, as_index=False)[metric_cols].sum(min_count=1)

print(f"➕ Aggregated into {len(agg_df):,} Snohomish rows")

# ------------------------------------------------------------
# BUILD SNOHOMISH ROWS
# ------------------------------------------------------------
agg_df["river"] = "Snohomish River"
agg_df["identifier"] = agg_df["Species_Plot"].apply(lambda s: f"Snohomish River - {s}")

# Ensure all columns are present; fill missing with blanks/NaN
for col in column_order:
    if col not in agg_df.columns:
        agg_df[col] = pd.NA

agg_df = agg_df[column_order]

# ------------------------------------------------------------
# APPEND AND SAVE
# ------------------------------------------------------------
df_final = pd.concat([df, agg_df], ignore_index=True)
print(f"📈 Final dataset size after Snohomish append: {len(df_final):,} rows")

df_final.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 85 complete — Snohomish River rows added to EscapementReport_PlotData.")
