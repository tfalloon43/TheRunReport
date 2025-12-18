# step82_Pink_correction.py
# ------------------------------------------------------------
# Step 82: Pink correction (10_year)
#
# Pink salmon generally return every other year; if a 10-year
# average is computed across calendar years, pink returns can be
# underestimated vs. “run-year” expectation. This step corrects
# for that by multiplying `10_year` for rows identified as Pink.
#
# Note: EscapementReport_PlotData does not currently include a
# dedicated `family` column. We infer “family” from (in order):
# - `family` / `Family` (if present)
# - `Species_Plot` (added in Step 81)
# - `identifier` (fallback: text after last " - ")
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

TEN_YEAR_MULTIPLIER = 2.0

print("🐟 Step 82: Applying Pink correction to 10_year in EscapementReport_PlotData...")

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

# ------------------------------------------------------------
# VALIDATE
# ------------------------------------------------------------
if "10_year" not in df.columns:
    raise ValueError("❌ Missing required column '10_year' in EscapementReport_PlotData.")

# ------------------------------------------------------------
# DETERMINE FAMILY SOURCE
# ------------------------------------------------------------
family_source_col = None
for candidate in ("family", "Family", "Species_Plot"):
    if candidate in df.columns:
        family_source_col = candidate
        break

if family_source_col is not None:
    family_text = df[family_source_col].astype("string")
else:
    if "identifier" not in df.columns:
        raise ValueError(
            "❌ Cannot infer family: missing 'family'/'Family'/'Species_Plot' and also missing 'identifier'."
        )

    family_text = (
        df["identifier"]
        .astype("string")
        .str.split(" - ")
        .str[-1]
    )
    family_source_col = "identifier (suffix after ' - ')"

family_norm = family_text.str.strip().str.casefold()
mask_pink = family_norm.str.contains("pink", regex=False, na=False)
rows_targeted = int(mask_pink.sum())

# ------------------------------------------------------------
# APPLY CORRECTION
# ------------------------------------------------------------
if rows_targeted == 0:
    print(f"ℹ️ No Pink rows found using {family_source_col}; no changes made.")
else:
    ten_year_numeric = pd.to_numeric(df.loc[mask_pink, "10_year"], errors="coerce")
    df.loc[mask_pink, "10_year"] = ten_year_numeric * TEN_YEAR_MULTIPLIER
    print(
        f"✅ Updated {rows_targeted:,} row(s): multiplied `10_year` by {TEN_YEAR_MULTIPLIER} "
        f"for Pink rows (detected via {family_source_col})."
    )

# ------------------------------------------------------------
# WRITE BACK
# ------------------------------------------------------------
df.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 82 complete — Pink correction applied.")
