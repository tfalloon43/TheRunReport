# step77_weekly.py
# ------------------------------------------------------------
# Step 77: Convert daily basinfamily table to weekly totals
# (with 12-31 partial-week adjustment ×3.5)
#
# Reads EscapementReports_dailycounts and outputs a weekly
# aggregation into EscapementReports_weeklycounts.
# At the end, all numeric values are rounded to 2 decimals.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("📆 Step 77: Converting daily basinfamily table to weekly totals...")

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
df = pd.read_sql_query("SELECT * FROM EscapementReports_dailycounts;", conn)
print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns from EscapementReports_dailycounts")

# ------------------------------------------------------------
# VALIDATE
# ------------------------------------------------------------
required = ["metric_type", "MM-DD"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in EscapementReports_dailycounts: {missing}")

# Identify numeric columns (all except metric_type, MM-DD)
numeric_cols = [c for c in df.columns if c not in ("metric_type", "MM-DD")]
if not numeric_cols:
    raise ValueError("❌ No basinfamily value columns found to aggregate.")

# Normalize columns
df["metric_type"] = df["metric_type"].astype(str).str.strip()
df["MM-DD"] = df["MM-DD"].astype(str).str.strip()
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)

metric_types = df["metric_type"].unique()
weekly_frames = []


def make_weekly(group_df: pd.DataFrame, metric_label: str) -> pd.DataFrame:
    """Aggregate daily data into 7-day blocks and apply 12-31 adjustment."""
    temp = group_df.copy()
    temp["date"] = pd.to_datetime("2024-" + temp["MM-DD"], errors="coerce")
    temp = temp.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    weekly_rows = []
    for start in range(0, len(temp), 7):
        chunk = temp.iloc[start : start + 7]
        if len(chunk) == 0:
            continue
        summed = chunk[numeric_cols].sum(numeric_only=True)
        last_day = chunk["date"].iloc[-1].strftime("%m-%d")
        weekly_rows.append([metric_label, last_day] + summed.tolist())

    weekly_df = pd.DataFrame(weekly_rows, columns=["metric_type", "MM-DD"] + numeric_cols)

    # Apply 12-31 adjustment ×3.5 to the final week if present
    if (weekly_df["MM-DD"] == "12-31").any():
        idx = weekly_df.index[weekly_df["MM-DD"] == "12-31"][0]
        adj = (weekly_df.loc[idx:idx, numeric_cols].astype(float) * 3.5)
        weekly_df.loc[idx:idx, numeric_cols] = adj.values
        print(f"🔧 Adjusted final week (12-31) for metric_type={metric_label} — multiplied by 3.5")

    return weekly_df


# ------------------------------------------------------------
# PROCESS EACH METRIC TYPE
# ------------------------------------------------------------
for mt in metric_types:
    subset = df[df["metric_type"] == mt]
    if subset.empty:
        continue
    weekly_frames.append(make_weekly(subset, mt))

if weekly_frames:
    weekly_df = pd.concat(weekly_frames, ignore_index=True)
else:
    weekly_df = pd.DataFrame(columns=["metric_type", "MM-DD"] + numeric_cols)
    print("ℹ️ No data available to produce weekly aggregation.")

# ------------------------------------------------------------
# ROUND NUMERICS
# ------------------------------------------------------------
if not weekly_df.empty and numeric_cols:
    weekly_df[numeric_cols] = weekly_df[numeric_cols].apply(pd.to_numeric, errors="coerce").round(2).fillna(0.0)

# ------------------------------------------------------------
# WRITE BACK TO DB
# ------------------------------------------------------------
weekly_df.to_sql("EscapementReports_weeklycounts", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Step 77 complete — weekly counts table generated.")
print(f"📊 Weekly rows: {len(weekly_df):,}")
print(f"🔢 Columns: {len(weekly_df.columns)}")
