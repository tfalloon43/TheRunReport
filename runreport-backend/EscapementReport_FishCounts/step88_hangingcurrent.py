# step88_hangingcurrent.py
# ------------------------------------------------------------
# Step 88: Prevent current_year from dropping to zero in the future
#
# Rules per (river, Species_Plot):
#   - If no fish have appeared yet this year (no fishperday > 0),
#     set current_year to NaN for all dates (hide the line).
#   - Otherwise keep zeros up to the latest reported date, and set
#     current_year to NaN after that date so the line stops.
# ------------------------------------------------------------

import sqlite3
from pathlib import Path

import pandas as pd

print("🧹 Step 88: Trimming current_year to reported window...")

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
plot_df = pd.read_sql_query("SELECT * FROM EscapementReport_PlotData;", conn)
src_df = pd.read_sql_query(
    "SELECT basinfamily, date_iso, fishperday FROM Escapement_PlotPipeline;",
    conn,
)
print(f"📂 Loaded {len(plot_df):,} rows from EscapementReport_PlotData")
print(f"📂 Loaded {len(src_df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
# VALIDATE
# ------------------------------------------------------------
plot_required = ["river", "Species_Plot", "MM-DD", "current_year"]
missing_plot = [c for c in plot_required if c not in plot_df.columns]
if missing_plot:
    raise ValueError(f"❌ Missing required columns in EscapementReport_PlotData: {missing_plot}")

src_required = ["basinfamily", "date_iso", "fishperday"]
missing_src = [c for c in src_required if c not in src_df.columns]
if missing_src:
    raise ValueError(f"❌ Missing required columns in Escapement_PlotPipeline: {missing_src}")

# ------------------------------------------------------------
# NORMALIZE
# ------------------------------------------------------------
plot_df["river"] = plot_df["river"].astype(str).str.strip()
plot_df["Species_Plot"] = plot_df["Species_Plot"].astype(str).str.strip()
plot_df["MM-DD"] = plot_df["MM-DD"].astype(str).str.strip()
plot_df["current_year"] = pd.to_numeric(plot_df["current_year"], errors="coerce")

plot_df["basinfamily"] = plot_df["river"] + " - " + plot_df["Species_Plot"]
plot_df["date_obj"] = pd.to_datetime("2024-" + plot_df["MM-DD"], errors="coerce")

src_df["basinfamily"] = src_df["basinfamily"].astype(str).str.strip()
src_df["date_iso"] = pd.to_datetime(src_df["date_iso"], errors="coerce")
src_df["fishperday"] = pd.to_numeric(src_df["fishperday"], errors="coerce")

current_year = pd.Timestamp.today().year
src_current = src_df[src_df["date_iso"].dt.year == current_year].copy()

# ------------------------------------------------------------
# BUILD REPORTED WINDOW + FISH PRESENCE MAP
# ------------------------------------------------------------
if src_current.empty:
    reported = pd.DataFrame(columns=["basinfamily", "last_reported_date", "has_fish"])
else:
    reported = (
        src_current.groupby("basinfamily", as_index=False)
        .agg(
            last_reported_date=("date_iso", "max"),
            has_fish=("fishperday", lambda s: (s > 0).any()),
        )
    )

reported["last_mmdd"] = reported["last_reported_date"].dt.strftime("%m-%d")
reported["last_date_obj"] = pd.to_datetime("2024-" + reported["last_mmdd"], errors="coerce")

# ------------------------------------------------------------
# APPLY TRIM LOGIC
# ------------------------------------------------------------
plot_df = plot_df.merge(
    reported[["basinfamily", "last_date_obj", "has_fish"]], on="basinfamily", how="left"
)

no_fish_or_no_reports = plot_df["has_fish"].isna() | (plot_df["has_fish"] == False)
plot_df.loc[no_fish_or_no_reports, "current_year"] = pd.NA

has_reported_window = plot_df["last_date_obj"].notna() & (plot_df["has_fish"] == True)
future_mask = has_reported_window & (plot_df["date_obj"] > plot_df["last_date_obj"])
plot_df.loc[future_mask, "current_year"] = pd.NA

# ------------------------------------------------------------
# CLEANUP + WRITE BACK
# ------------------------------------------------------------
plot_df = plot_df.drop(columns=["basinfamily", "date_obj", "last_date_obj", "has_fish"])
plot_df.to_sql("EscapementReport_PlotData", conn, if_exists="replace", index=False)
conn.close()

print("✅ Step 88 complete — current_year trimmed to reported window.")
