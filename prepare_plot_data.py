# prepare_plot_data.py
import sqlite3
import pandas as pd
import numpy as np

db_path = "pdf_data.sqlite"
conn = sqlite3.connect(db_path)

# 1) Load cleaned table
df = pd.read_sql("SELECT * FROM escapement_data_cleaned", conn)

# 2) Build reliable datetime from date_iso (fallback to 'date' if needed)
if "date_iso" in df.columns:
    dt = pd.to_datetime(df["date_iso"], errors="coerce")
else:
    dt = pd.Series(pd.NaT, index=df.index)

need_fb = dt.isna()
if need_fb.any():
    dt_fb = pd.to_datetime(df.loc[need_fb, "date"], format="%m/%d/%y", errors="coerce")
    still = dt_fb.isna()
    if still.any():
        dt_fb.loc[still] = pd.to_datetime(
            df.loc[need_fb, "date"][still], format="%m/%d/%Y", errors="coerce"
        )
    dt.loc[need_fb] = dt_fb

df["date_iso"] = dt.dt.strftime("%Y-%m-%d")
df["_dt"] = dt

# 3) Clean key columns
df["hatch_name"] = df["hatch_name"].fillna(df["hatchery"].fillna("").str.title())
df["species"] = df["species"].fillna("").astype(str)
df["stock"] = df["stock"].fillna("").astype(str)

# --- Stock ordering function
def stock_sort_val(stock):
    if stock == "H":
        return 0
    elif stock == "W":
        return 1
    elif stock == "U":
        return 2
    else:
        return 3

df["stock_order"] = df["stock"].apply(stock_sort_val)

# 4) Sort by hatch_name → species → stock_order → date
df_sorted = (
    df.sort_values(by=["hatch_name", "species", "stock_order", "_dt"])
      .reset_index(drop=True)
)

# 5) Convert adult_total to numeric (handle commas)
df_sorted["adult_total_num"] = (
    df_sorted["adult_total"].astype(str)
    .str.replace(",", "", regex=False)
    .str.extract(r"(\d+\.?\d*)")[0]
    .astype(float)
)

# 6) Compute initial diffs
df_sorted["day_diff"] = (
    df_sorted.groupby(["hatch_name", "species", "stock"])["_dt"]
    .diff()
    .dt.days
    .fillna(0)
    .astype(int)
)
df_sorted["adult_diff"] = (
    df_sorted.groupby(["hatch_name", "species", "stock"])["adult_total_num"]
    .diff()
    .fillna(df_sorted["adult_total_num"])
)

# 7) Biological season logic
df_sorted["season_year"] = 0
for (hatch, spec, stock), idxs in df_sorted.groupby(["hatch_name", "species", "stock"]).groups.items():
    group = df_sorted.loc[idxs].sort_values("_dt")
    season = 1
    season_values = []
    prev_row = None
    for _, row in group.iterrows():
        if prev_row is not None:
            day_gap = (row["_dt"] - prev_row["_dt"]).days
            adult_drop = row["adult_total_num"] < prev_row["adult_total_num"]
            # Reset if big gap + drop, or huge gap alone (>150 days)
            if (day_gap > 90 and adult_drop) or (day_gap > 150):
                season += 1
        season_values.append(season)
        prev_row = row
    df_sorted.loc[idxs, "season_year"] = season_values

# 8) Reset on hatch_name, species, or stock change (ensures fresh numbering)
group_change = (
    (df_sorted["hatch_name"] != df_sorted["hatch_name"].shift()) |
    (df_sorted["species"] != df_sorted["species"].shift()) |
    (df_sorted["stock"] != df_sorted["stock"].shift())
)
df_sorted.loc[group_change, "season_year"] = 1

# 9) Re-sort within season and recompute final diffs
df_sorted = df_sorted.sort_values(
    by=["hatch_name", "species", "stock_order", "season_year", "_dt"]
).reset_index(drop=True)

df_sorted["day_diff"] = (
    df_sorted.groupby(["hatch_name", "species", "stock", "season_year"])["_dt"]
    .diff()
    .dt.days
    .fillna(0)
    .astype(int)
)

df_sorted["adult_diff"] = (
    df_sorted.groupby(["hatch_name", "species", "stock", "season_year"])["adult_total_num"]
    .diff()
    .fillna(df_sorted["adult_total_num"])
)

# 10) Drop duplicates (same hatch_name + stock + adult_total + season_year)
df_sorted = df_sorted.drop_duplicates(
    subset=["hatch_name", "stock", "adult_total_num", "season_year"],
    keep="first"
)

# 11) Save reordered and reduced data
df_sorted.drop(columns=["_dt", "stock_order"]).to_sql(
    "escapement_reordered", conn, if_exists="replace", index=False
)
df_sorted.drop(columns=["_dt", "stock_order"]).to_csv(
    "escapement_reordered.csv", index=False
)

reduced = df_sorted.drop(columns=["_dt", "stock_order"], errors="ignore")
reduced.to_sql("escapement_reduced", conn, if_exists="replace", index=False)
reduced.to_csv("escapement_reduced.csv", index=False)

conn.close()

print("✅ escapement_reordered and escapement_reduced created with full season-year reset logic (90+drop or 150+gap).")