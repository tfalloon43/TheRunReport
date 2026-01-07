# step76_tablefill.py
# ------------------------------------------------------------
# Step 76: Fill EscapementReports_dailycounts with fishperday values
#
# For each row in Escapement_PlotPipeline:
#   - Get fishperday and DayN values.
#   - Add fishperday to the matching Day row and basinfamily column
#     in EscapementReports_dailycounts.
#   - Target metric_type is determined by the inferred calendar year
#     for each DayN MM-DD value:
#       * If MM-DD wraps past 01-01 when counting backwards (e.g., a
#         current-year early-January date has DayN values like 12-31),
#         those wrapped days are treated as belonging to the prior year.
#       * current year → current_year
#       * previous year → previous_year AND 10_year
#       * earlier years → 10_year only
# After processing, all metric_type == 10_year values are averaged by /10.
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

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

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def infer_calendar_year_for_mmdd(mmdd: str, end_date: pd.Timestamp) -> int:
    """
    Given a DayN value like '12-31' and an end date (date_iso),
    infer which calendar year that day belongs to when counting
    backwards from end_date.

    Rule: if mmdd is later in the calendar than end_date's MM-DD,
    it must have wrapped into the prior year.
    """
    end_mmdd = end_date.strftime("%m-%d")
    return end_date.year - 1 if mmdd > end_mmdd else end_date.year

# ------------------------------------------------------------
# CORE UPDATE (VECTORIZED)
# ------------------------------------------------------------
# Long-form DayN values: one row per (basinfamily, date_iso, MM-DD)
long_df = (
    source_df[["basinfamily", "fishperday", "date_iso"] + day_cols]
    .melt(
        id_vars=["basinfamily", "fishperday", "date_iso"],
        value_vars=day_cols,
        value_name="MM-DD",
    )
    .drop(columns=["variable"])
)

# Clean and filter
long_df["MM-DD"] = long_df["MM-DD"].astype(str).str.strip()
long_df = long_df[
    long_df["MM-DD"].str.match(r"^\d{2}-\d{2}$", na=False)
    & long_df["fishperday"].notna()
    & (long_df["fishperday"] != 0)
    & long_df["date_iso"].notna()
]
long_df = long_df[long_df["basinfamily"].isin(target_value_columns)]

# Infer calendar year in bulk (wrap if MM-DD is later than end date MM-DD)
end_mmdd = long_df["date_iso"].dt.strftime("%m-%d")
inferred_year = long_df["date_iso"].dt.year - (long_df["MM-DD"] > end_mmdd)

long_df["metric_type"] = ""
long_df.loc[inferred_year == current_year, "metric_type"] = "current_year"
long_df.loc[inferred_year == previous_year, "metric_type"] = "previous_year|10_year"
long_df.loc[inferred_year < previous_year, "metric_type"] = "10_year"

# Expand metric_type so previous_year contributes to both previous_year and 10_year
long_df["metric_type"] = long_df["metric_type"].str.split("|")
long_df = long_df.explode("metric_type")

# Aggregate additions
adds = (
    long_df.groupby(["metric_type", "MM-DD", "basinfamily"], as_index=False)["fishperday"]
    .sum()
)

# Pivot to align with template, then add to the table in one pass
adds_wide = (
    adds.pivot(index=["metric_type", "MM-DD"], columns="basinfamily", values="fishperday")
    .reindex(columns=target_value_columns)
)

table_key = table_df.set_index(["metric_type", "MM-DD"])
table_vals = (
    table_key[target_value_columns]
    .apply(pd.to_numeric, errors="coerce")
    .fillna(0)
)
table_vals = table_vals.add(adds_wide, fill_value=0)
table_key[target_value_columns] = table_vals
table_df = table_key.reset_index()

rows_processed = int(len(source_df))
cells_updated = int(adds["fishperday"].notna().sum())

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
