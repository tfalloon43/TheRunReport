# build_plots.py
import sqlite3
import pandas as pd
import numpy as np

db_path = "pdf_data.sqlite"
conn = sqlite3.connect(db_path)

# ------------------------------------------------------------
# 1. Load raw escapement_data
# ------------------------------------------------------------
df = pd.read_sql("SELECT * FROM escapement_data", conn)

# --- Parse dates robustly ---
# First try MM/DD/YY
df["date"] = pd.to_datetime(df["date"], format="%m/%d/%y", errors="coerce")

# If some are still NaT, try MM/DD/YYYY
mask = df["date"].isna()
if mask.any():
    df.loc[mask, "date"] = pd.to_datetime(
        df.loc[mask, "date"], format="%m/%d/%Y", errors="coerce"
    )

# Drop rows with invalid dates after both attempts
df = df.dropna(subset=["date"])

# Only keep needed columns
df = df[["date", "species", "family", "basin", "stock", "adult_total"]]

# Ensure adult_total numeric
df["adult_total"] = pd.to_numeric(df["adult_total"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# 2. Convert cumulative totals → daily new counts
# ------------------------------------------------------------
df = df.sort_values(["basin", "family", "stock", "date"])

df["daily_new"] = df.groupby(["basin", "family", "stock"])["adult_total"].diff().fillna(df["adult_total"])

# Fix negatives (sometimes reports reset or glitch)
df.loc[df["daily_new"] < 0, "daily_new"] = 0

# Save escapement_daily
df_daily = df[["date", "basin", "family", "stock", "daily_new"]].copy()
df_daily["year"] = df_daily["date"].dt.year
df_daily["month"] = df_daily["date"].dt.month
df_daily["day"] = df_daily["date"].dt.day

df_daily.to_sql("escapement_daily", conn, if_exists="replace", index=False)

# ------------------------------------------------------------
# 3. Aggregate into 4 “weeks” per month
# ------------------------------------------------------------
def week_in_month(d):
    """Return week number in month (1-4, sometimes 5)."""
    return (d.day - 1) // 7 + 1

df_daily["week_in_month"] = df_daily["date"].apply(week_in_month)

df_weekly = (
    df_daily.groupby(["year", "month", "week_in_month", "basin", "family", "stock"])["daily_new"]
    .sum()
    .reset_index()
    .rename(columns={"daily_new": "weekly_total"})
)

df_weekly.to_sql("escapement_weekly", conn, if_exists="replace", index=False)

# ------------------------------------------------------------
# 3a. Clean weekly table (deduplicate + sort)
# ------------------------------------------------------------
df_weekly_clean = (
    df_weekly
    .drop_duplicates(subset=["year", "month", "week_in_month", "basin", "family", "stock"])
    .sort_values(by=["basin", "family", "stock", "year", "month", "week_in_month"])
)

df_weekly_clean.to_sql("escapement_weekly_clean", conn, if_exists="replace", index=False)

# ------------------------------------------------------------
# 4. Historical averages (10-year window)
# ------------------------------------------------------------
df_weekly_avg = (
    df_weekly.groupby(["month", "week_in_month", "basin", "family", "stock"])["weekly_total"]
    .mean()
    .reset_index()
    .rename(columns={"weekly_total": "avg_weekly_total"})
)

df_weekly_avg.to_sql("escapement_weekly_avg", conn, if_exists="replace", index=False)

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
conn.close()
print("Transforms complete: escapement_daily, escapement_weekly, escapement_weekly_clean, escapement_weekly_avg created")
