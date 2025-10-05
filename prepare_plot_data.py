# prepare_plot_data.py
import sqlite3
import pandas as pd

db_path = "pdf_data.sqlite"
conn = sqlite3.connect(db_path)

# 1) Load cleaned table
df = pd.read_sql("SELECT * FROM escapement_data_cleaned", conn)

# 2) Build a reliable datetime from date_iso (fallback to 'date' if needed)
if "date_iso" in df.columns:
    dt = pd.to_datetime(df["date_iso"], errors="coerce")
else:
    dt = pd.Series(pd.NaT, index=df.index)

need_fb = dt.isna()
if need_fb.any():
    dt_fb = pd.to_datetime(df.loc[need_fb, "date"], format="%m/%d/%y", errors="coerce")
    still = dt_fb.isna()
    if still.any():
        dt_fb.loc[still] = pd.to_datetime(df.loc[need_fb, "date"][still], format="%m/%d/%Y", errors="coerce")
    dt.loc[need_fb] = dt_fb

df["date_iso"] = dt.dt.strftime("%Y-%m-%d")

# Ensure grouping keys exist and are tidy
if "hatch_name" not in df.columns:
    df["hatch_name"] = df["hatchery"].fillna("").str.title()
else:
    df["hatch_name"] = df["hatch_name"].fillna(df["hatchery"].fillna("").str.title())

df["basin"] = df["basin"].fillna("").astype(str)
df["family"] = df["family"].fillna("").astype(str)
df["stock"] = df["stock"].fillna("").astype(str)

df["_dt"] = dt

# 3) Sort by basin → hatch_name → family → stock → date
df_sorted = (
    df.sort_values(by=["basin", "hatch_name", "family", "stock", "_dt"])
      .reset_index(drop=True)
)

# 4) Compute "days_since_last"
df_sorted["days_since_last"] = (
    df_sorted.groupby(["basin", "hatch_name", "family", "stock"])["_dt"]
    .diff()
    .dt.days
)

# Replace NaN (first record in each group) with 0
df_sorted["days_since_last"] = df_sorted["days_since_last"].fillna(0).astype(int)

# 5) Save for inspection
df_sorted.drop(columns=["_dt"]).to_sql("escapement_reordered", conn, if_exists="replace", index=False)

conn.close()
print("Reordered table 'escapement_reordered' created with 'days_since_last' column.")