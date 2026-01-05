# step35_iteration2.py
# ------------------------------------------------------------
# Step 35: Biological Metrics Iteration 2
#
# Recomputes the following **inside the database**:
#
#   day_diff2
#   adult_diff2
#   by_adult2
#   by_adult2_length
#   by_short2
#   x_count2
#
# All computations use Escapement_PlotPipeline and rewrite it.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path


# ------------------------------------------------------------
# Reorder helper
# ------------------------------------------------------------

def reorder_for_output(df):
    sort_cols = ["facility", "species", "Stock", "Stock_BO", "date_iso", "Adult_Total"]
    missing = [c for c in sort_cols if c not in df.columns]
    if missing:
        return df
    df = df.copy()
    df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
    df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)
    return df.sort_values(
        by=sort_cols,
        ascending=[True, True, True, True, True, False],
        na_position="last",
        kind="mergesort",
    )


print("🏗️ Step 35: Recomputing biological metrics (Iteration 2)...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"

print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD TABLE
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
rename_map = {
    "Adult Total": "Adult_Total",
    "Jack Total": "Jack_Total",
    "Total Eggtake": "Total_Eggtake",
    "On Hand Adults": "On_Hand_Adults",
    "On Hand Jacks": "On_Hand_Jacks",
    "Lethal Spawned": "Lethal_Spawned",
    "Live Spawned": "Live_Spawned",
    "Live Shipped": "Live_Shipped",
}
df = df.rename(columns=rename_map)

# REQUIRED COLUMN CHECK
# ------------------------------------------------------------
required_cols = [
    "facility",
    "species",
    "Stock",
    "Stock_BO",
    "date_iso",
    "Adult_Total"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ============================================================
# STEP 1 — day_diff2
# ============================================================
print("🔹 Calculating day_diff2...")

df["day_diff2"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

# ============================================================
# STEP 2 — adult_diff2
# ============================================================
print("🔹 Calculating adult_diff2...")

df["adult_diff2"] = df.groupby(group_cols)["Adult_Total"].diff()

# Reset adult_diff2 for new groups (first row in each group)
first_in_group = df.groupby(group_cols).cumcount() == 0
df.loc[first_in_group, "adult_diff2"] = df.loc[first_in_group, "Adult_Total"]
df["adult_diff2"] = df["adult_diff2"].fillna(df["Adult_Total"])

# ============================================================
# STEP 3 — by_adult2
# ============================================================
print("🔹 Assigning by_adult2...")

trigger = (df["adult_diff2"] < 0) | (df["day_diff2"] > 90)
trigger = trigger & ~first_in_group
df["by_adult2"] = trigger.groupby([df[c] for c in group_cols]).cumsum() + 1

# ============================================================
# STEP 4 — by_adult2_length
# ============================================================
print("🔹 Calculating by_adult2_length...")

lengths = (
    df.groupby(group_cols + ["by_adult2"])
    .size()
    .reset_index(name="by_adult2_length")
)

df = df.merge(lengths, on=group_cols + ["by_adult2"], how="left")

# ============================================================
# STEP 5 — by_short2
# ============================================================
print("🔹 Detecting short spillover runs (by_short2)...")

df["by_short2"] = ""

def detect_spill2(g):
    g = g.reset_index(drop=True)
    stock_type = str(g.loc[0, "Stock"]).strip().upper()

    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()
    runs = g[["by_adult2", "by_adult2_length"]].drop_duplicates().reset_index(drop=True)

    for i, r in runs.iterrows():
        curr_len = r["by_adult2_length"]

        if curr_len > 15:
            lookahead = runs.iloc[i+1:i+5]

            for _, nxt in lookahead.iterrows():
                next_len = nxt["by_adult2_length"]
                next_by = nxt["by_adult2"]

                sub = g[g["by_adult2"] == next_by]

                if (sub["day_diff2"] > 250).any():
                    break

                if next_len < 5:
                    short_idx.update(sub.index)
                else:
                    break

    g.loc[g.index.isin(short_idx), "by_short2"] = "X"
    return g


df = (
    df.groupby(group_cols, group_keys=False)
      .apply(detect_spill2)
      .reset_index(drop=True)
)

# ============================================================
# STEP 6 — x_count2
# ============================================================
print("🔹 Counting contiguous X sequences (x_count2)...")

mask = df["by_short2"] == "X"
group_boundary = df[group_cols].ne(df[group_cols].shift()).any(axis=1)
run_id = (mask.ne(mask.shift()) | group_boundary).cumsum()
run_len = df.groupby(run_id)["by_short2"].transform("size")
df["x_count2"] = run_len.where(mask, 0).astype(int)

# ============================================================
# SAVE BACK TO DB
# ============================================================
print("💾 Writing iteration2 metrics back to Escapement_PlotPipeline...")

df = reorder_for_output(df)

df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Iteration 2 Complete!")
print(f"📊 Rows processed: {len(df):,}")
print(f"📈 Short runs flagged: {(df['by_short2'] == 'X').sum():,}")
print(f"🔢 Max biological year (iteration2): {df['by_adult2'].max()}")
print("🏁 Biological metrics iteration 2 successfully updated.")
