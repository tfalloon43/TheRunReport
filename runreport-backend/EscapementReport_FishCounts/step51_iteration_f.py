# step51_iteration_f.py
# ------------------------------------------------------------
# Step 51 (Final): Biological Metrics Iteration F (_f)
#
# Recomputes the final biological metrics inside the DB:
#
#   day_diff_f
#   adult_diff_f
#   by_adult_f
#   by_adult_f_length
#   by_short_f
#   x_count_f
#
# Reads + rewrites Escapement_PlotPipeline.
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


print("🏗️ Step 51: Recomputing FINAL biological metrics (Iteration F)...")

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
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# Normalize column names to underscore schema if needed
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

# ------------------------------------------------------------
# REQUIRED COLUMNS
# ------------------------------------------------------------
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "Family", "date_iso", "Adult_Total"
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

# Stable sort
df = df.reset_index(drop=True)
if "index" not in df.columns:
    df = df.reset_index()
df = df.sort_values(group_cols + ["date_iso", "index"]).reset_index(drop=True)

# ============================================================
# STEP 1: day_diff_f and adult_diff_f
# ============================================================
print("🔹 Calculating day_diff_f and adult_diff_f...")

df["day_diff_f"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

df["adult_diff_f"] = df.groupby(group_cols)["Adult_Total"].diff()

# Reset diffs at group boundaries (first row in each group)
first_in_group = df.groupby(group_cols).cumcount() == 0
df.loc[first_in_group, "adult_diff_f"] = df.loc[first_in_group, "Adult_Total"]
df["adult_diff_f"] = df["adult_diff_f"].fillna(df["Adult_Total"])

# ============================================================
# STEP 2: by_adult_f assignment
# ============================================================
print("🔹 Assigning by_adult_f...")

trigger = (df["adult_diff_f"] < 0) | (df["day_diff_f"] > 90)
trigger = trigger & ~first_in_group
df["by_adult_f"] = trigger.groupby([df[c] for c in group_cols]).cumsum() + 1

# ============================================================
# RESET DIFFS/DAYS AT BIOLOGICAL-YEAR BOUNDARIES
# ============================================================
print("🔹 Resetting diffs for new biological years...")

df["by_change"] = df.groupby(group_cols)["by_adult_f"].diff().fillna(0)
boundary_mask = df["by_change"] != 0

df.loc[boundary_mask, "day_diff_f"] = 7
df.loc[boundary_mask, "adult_diff_f"] = df.loc[boundary_mask, "Adult_Total"]

df = df.drop(columns=["by_change"])

# ============================================================
# STEP 3: by_adult_f_length
# ============================================================
print("🔹 Calculating by_adult_f_length...")

lengths = (
    df.groupby(group_cols + ["by_adult_f"])
    .size()
    .reset_index(name="by_adult_f_length")
)

df = df.merge(lengths, on=group_cols + ["by_adult_f"], how="left")

# ============================================================
# STEP 4: by_short_f
# ============================================================
print("🔹 Detecting spillover short runs (by_short_f, salmonids only)...")

df["by_short_f"] = ""
valid_families = ["Steelhead", "Chinook", "Coho", "Chum", "Pink", "Sockeye"]

def detect_spillover(g):
    g = g.reset_index(drop=True)

    family = str(g.loc[0, "Family"]).strip().title()
    if family not in valid_families:
        return g

    stock_type = str(g.loc[0, "Stock"]).strip().upper()
    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()
    runs = g[["by_adult_f", "by_adult_f_length"]].drop_duplicates().reset_index(drop=True)

    for i, r in runs.iterrows():
        curr_len = r["by_adult_f_length"]

        if curr_len > 15:
            lookahead = runs.iloc[i+1:i+5]

            for _, nxt in lookahead.iterrows():
                next_len = nxt["by_adult_f_length"]
                next_by = nxt["by_adult_f"]

                sub = g[g["by_adult_f"] == next_by]

                if (sub["day_diff_f"] > 250).any():
                    break

                if next_len < 5:
                    short_idx.update(sub.index)
                else:
                    break

    g.loc[g.index.isin(short_idx), "by_short_f"] = "X"
    return g


df = (
    df.groupby(group_cols, group_keys=False)
      .apply(detect_spillover)
      .reset_index(drop=True)
)

# ============================================================
# STEP 5: x_count_f
# ============================================================
print("🔹 Counting contiguous X sequences (x_count_f)...")

mask = df["by_short_f"] == "X"
group_boundary = df[group_cols + ["by_adult_f"]].ne(df[group_cols + ["by_adult_f"]].shift()).any(axis=1)
run_id = (mask.ne(mask.shift()) | group_boundary).cumsum()
run_len = df.groupby(run_id)["by_short_f"].transform("size")
df["x_count_f"] = run_len.where(mask, 0).astype(int)

# ============================================================
# FINAL SORT & SAVE
# ============================================================
df = df.sort_values(group_cols + ["by_adult_f", "date_iso", "index"]).reset_index(drop=True)

print("💾 Writing final biological metrics back to database...")
df = reorder_for_output(df)

df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Final Iteration (F) Complete!")
print(f"📊 Rows processed: {len(df):,}")
print(f"📈 Short runs flagged: {(df['by_short_f'] == 'X').sum():,}")
print(f"🔢 Max biological year: {df['by_adult_f'].max()}")
print("🏁 Final biological metrics successfully updated.")
