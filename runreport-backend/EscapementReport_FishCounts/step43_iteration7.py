# step43_iteration7.py
# ------------------------------------------------------------
# Step 43: Biological Metrics Iteration 7
#
# Recomputes inside the DB:
#   day_diff7
#   adult_diff7
#   by_adult7
#   by_adult7_length
#   by_short7
#   x_count7
#
# Reads + rewrites Escapement_PlotPipeline.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 43: Recomputing biological metrics (Iteration 7)...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD TABLE FROM DB
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)
df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)

print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
# CLEAN STRAY COLUMNS
# ------------------------------------------------------------
for col in df.columns:
    if col.lower().startswith("unnamed") or col == "level_0":
        print(f"🧹 Dropping stray column: {col}")
        df = df.drop(columns=[col])

# ------------------------------------------------------------
# REQUIRED COLUMNS
# ------------------------------------------------------------
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "Family", "date_iso", "Adult Total"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in DB: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult Total"] = pd.to_numeric(df["Adult Total"], errors="coerce").fillna(0)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# Stable sort
df = df.reset_index().sort_values(group_cols + ["date_iso", "index"]).reset_index(drop=True)

# ============================================================
# STEP 1 — day_diff7 and adult_diff7
# ============================================================
print("🔹 Calculating day_diff7 and adult_diff7...")

df["day_diff7"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

# Keep negative diffs
df["adult_diff7"] = df.groupby(group_cols)["Adult Total"].diff()

# Reset when group changes
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)
df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)

df.loc[df["group_changed"], "adult_diff7"] = df.loc[df["group_changed"], "Adult Total"]
df["adult_diff7"] = df["adult_diff7"].fillna(df["Adult Total"])

df = df.drop(columns=[f"{col}_changed" for col in group_cols] + ["group_changed"])

# ============================================================
# STEP 2 — by_adult7
# ============================================================
print("🔹 Assigning by_adult7...")

df = df.sort_values(group_cols + ["date_iso", "index"]).reset_index(drop=True)

by_adult7 = []
current = 1
prev_keys = None

for i, row in df.iterrows():
    keys = tuple(row[col] for col in group_cols)

    if keys != prev_keys:
        current = 1
        prev_keys = keys
    elif row["adult_diff7"] < 0 or row["day_diff7"] > 60:
        current += 1

    by_adult7.append(current)

df["by_adult7"] = by_adult7

# ============================================================
# STEP 3 — by_adult7_length
# ============================================================
print("🔹 Calculating by_adult7_length...")

lengths = (
    df.groupby(group_cols + ["by_adult7"])
    .size()
    .reset_index(name="by_adult7_length")
)

df = df.merge(lengths, on=group_cols + ["by_adult7"], how="left")

# ============================================================
# STEP 4 — by_short7
# ============================================================
print("🔹 Detecting short spillover runs (by_short7)...")

df["by_short7"] = ""

valid_families = ["Steelhead", "Chinook", "Coho", "Chum", "Pink", "Sockeye"]

def detect_spill7(g):
    g = g.sort_values("date_iso").reset_index(drop=True)

    family = str(g.loc[0, "Family"]).strip().title()
    if family not in valid_families:
        return g

    stock_type = str(g.loc[0, "Stock"]).strip().upper()
    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()
    runs = g[["by_adult7", "by_adult7_length"]].drop_duplicates().reset_index(drop=True)

    for i, row in runs.iterrows():
        curr_len = row["by_adult7_length"]

        if curr_len > 15:
            lookahead = runs.iloc[i+1:i+5]

            for _, nxt in lookahead.iterrows():
                next_len = nxt["by_adult7_length"]
                next_by = nxt["by_adult7"]

                sub = g[g["by_adult7"] == next_by]

                if (sub["day_diff7"] > 250).any():
                    break

                if next_len < 5:
                    short_idx.update(sub.index)
                else:
                    break

    g.loc[g.index.isin(short_idx), "by_short7"] = "X"
    return g

df = (
    df.groupby(group_cols, group_keys=False)
      .apply(detect_spill7)
      .reset_index(drop=True)
)

# ============================================================
# STEP 5 — x_count7
# ============================================================
print("🔹 Counting contiguous X sequences (x_count7)...")

df["x_count7"] = 0

def count_x7(g):
    g = g.sort_values(["date_iso", "index"]).reset_index(drop=True)

    counts = [0] * len(g)
    i = 0

    while i < len(g):
        if g.loc[i, "by_short7"] == "X":
            j = i
            while j < len(g) and g.loc[j, "by_short7"] == "X":
                j += 1

            length = j - i
            for k in range(i, j):
                counts[k] = length

            i = j
        else:
            i += 1

    g["x_count7"] = counts
    return g

df = (
    df.groupby(group_cols + ["by_adult7"], group_keys=False)
      .apply(count_x7)
      .reset_index(drop=True)
)

# ============================================================
# FINAL SORT & SAVE TO DB
# ============================================================
df = df.sort_values(group_cols + ["by_adult7", "date_iso", "index"]).reset_index(drop=True)

print("💾 Writing iteration7 metrics back to Escapement_PlotPipeline...")

df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Iteration 7 Complete!")
print(f"📊 Rows processed: {len(df):,}")
print(f"📈 Short runs flagged: {(df['by_short7'] == 'X').sum():,}")
print(f"🔢 Max biological year (iteration7): {df['by_adult7'].max()}")
print("🏁 Biological metrics iteration 7 successfully updated.")