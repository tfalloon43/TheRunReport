# step76_tablefill.py
# ------------------------------------------------------------
# Step 76: Fill EscapementReports_dailycounts with fishperday values
#
# For each row in Escapement_PlotPipeline:
#   - Get fishperday and DayN values.
#   - Add fishperday to the matching Day row and basinfamily column
#     in EscapementReports_dailycounts.
#   - Target metric_type is determined by the date year:
#       * current year → current_year
#       * previous year → previous_year AND 10_year
#       * earlier years → 10_year only
# After processing, all metric_type == 10_year values are averaged by /10.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 76: Filling EscapementReports_dailycounts with fishperday values...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD SOURCE AND TARGET TABLES
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
source_df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)
table_df = pd.read_sql_query("SELECT * FROM EscapementReports_dailycounts;", conn)

print(f"✅ Loaded {len(source_df):,} source rows (Escapement_PlotPipeline)")
print(f"✅ Loaded template with {len(table_df):,} rows and {len(table_df.columns)} columns (EscapementReports_dailycounts)")

# ------------------------------------------------------------
# VALIDATIONS
# ------------------------------------------------------------
required_source = ["basinfamily", "fishperday", "date_iso"]
missing_source = [c for c in required_source if c not in source_df.columns]
if missing_source:
    raise ValueError(f"❌ Missing required source columns: {missing_source}")

required_table = ["metric_type", "MM-DD"]
missing_table = [c for c in required_table if c not in table_df.columns]
if missing_table:
    raise ValueError(f"❌ Missing required table columns: {missing_table}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
source_df["fishperday"] = pd.to_numeric(source_df["fishperday"], errors="coerce")
source_df["date_iso"] = pd.to_datetime(source_df["date_iso"], errors="coerce")
source_df["basinfamily"] = source_df["basinfamily"].astype(str).str.strip()

table_df["metric_type"] = table_df["metric_type"].astype(str).str.strip()
table_df["MM-DD"] = table_df["MM-DD"].astype(str).str.strip()

# Identify basinfamily columns in the target table
target_value_columns = [c for c in table_df.columns if c not in ("metric_type", "MM-DD")]

current_year = pd.Timestamp.today().year
previous_year = current_year - 1

# Identify Day columns in the source
day_cols = [c for c in source_df.columns if c.lower().startswith("day")]

rows_processed = 0
cells_updated = 0

# ------------------------------------------------------------
# CORE UPDATE LOOP
# ------------------------------------------------------------
for _, row in source_df.iterrows():
    fish_value = row["fishperday"]
    date_val = row["date_iso"]
    basinfamily = row["basinfamily"]

    if pd.isna(fish_value) or fish_value == 0 or pd.isna(date_val):
        continue

    if basinfamily not in target_value_columns:
        # Skip rows whose basinfamily isn't in the template
        continue

    year = date_val.year
    if year == current_year:
        targets = ["current_year"]
    elif year == previous_year:
        targets = ["previous_year", "10_year"]
    else:
        targets = ["10_year"]

    # Gather valid day values
    days = [str(row[c]).strip() for c in day_cols if isinstance(row[c], str) and row[c].strip()]
    if not days:
        continue

    for d in days:
        # For each metric_type target, add the fishperday value
        for metric in targets:
            mask = (table_df["metric_type"] == metric) & (table_df["MM-DD"] == d)
            if not mask.any():
                continue

            current_vals = pd.to_numeric(table_df.loc[mask, basinfamily], errors="coerce").fillna(0)
            table_df.loc[mask, basinfamily] = current_vals + fish_value
            cells_updated += int(mask.sum())

    rows_processed += 1

# ------------------------------------------------------------
# AVERAGE 10-YEAR CELLS
# ------------------------------------------------------------
mask_10 = table_df["metric_type"] == "10_year"
if mask_10.any() and target_value_columns:
    sub = (
        table_df.loc[mask_10, target_value_columns]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        / 10
    )
    table_df.loc[mask_10, target_value_columns] = sub

# ------------------------------------------------------------
# WRITE BACK TO DB
# ------------------------------------------------------------
table_df.to_sql("EscapementReports_dailycounts", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Step 76 complete — daily counts table filled.")
print(f"📊 Rows processed: {rows_processed:,}")
print(f"➕ Cells updated: {cells_updated:,}")
