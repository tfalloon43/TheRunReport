import sqlite3
import pandas as pd

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

# Ensure grouping keys exist and are tidy
df["hatch_name"] = df["hatch_name"].fillna(df["hatchery"].fillna("").str.title())
df["species"] = df["species"].fillna("").astype(str)
df["stock"] = df["stock"].fillna("").astype(str)

# --- Stock ordering function
def stock_sort_val(stock):
    if stock in ("H", "U"):
        return 0
    elif stock == "W":
        return 1
    else:
        return 2

df["stock_order"] = df["stock"].apply(stock_sort_val)

# 3) Sort by hatch_name → species → stock_order → date
df_sorted = (
    df.sort_values(by=["hatch_name", "species", "stock_order", "_dt"])
      .reset_index(drop=True)
)

# 4) Compute days_since_last
df_sorted["days_since_last"] = (
    df_sorted.groupby(["hatch_name", "species", "stock_order"])["_dt"]
    .diff()
    .dt.days
)
df_sorted["days_since_last"] = df_sorted["days_since_last"].fillna(0).astype(int)

# 5) Save reordered full dataset
df_sorted.drop(columns=["_dt", "stock_order"]).to_sql(
    "escapement_reordered", conn, if_exists="replace", index=False
)
df_sorted.drop(columns=["_dt", "stock_order"]).to_csv(
    "escapement_reordered.csv", index=False
)

# -------------------------------
# 6) Build escapement_reduced (drop duplicates only)
# -------------------------------
reduced = df_sorted.drop(columns=["_dt", "stock_order"]).drop_duplicates(
    subset=["hatch_name", "species", "stock", "date_iso", "adult_total"]
)

# Save reduced version
reduced.to_sql("escapement_reduced", conn, if_exists="replace", index=False)
reduced.to_csv("escapement_reduced.csv", index=False)

conn.close()

print("✅ escapement_reordered and escapement_reduced created successfully (no duplicate rows).")
