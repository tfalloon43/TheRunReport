# step39_iteration4.py
# ------------------------------------------------------------
# Step 39: Biological Metrics Iteration 4
#
# Recomputes the following **inside the database**:
#
#   day_diff4
#   adult_diff4
#   by_adult4
#   by_adult4_length
#   by_short4
#   x_count4
#
# All computations read + rewrite Escapement_PlotPipeline.
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


print("🏗️ Step 39: Recomputing biological metrics (Iteration 4)...")

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
# REQUIRED COLUMNS
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
# STEP 1 — day_diff4
# ============================================================
print("🔹 Calculating day_diff4...")

df["day_diff4"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

# ============================================================
# STEP 2 — adult_diff4
# ============================================================
print("🔹 Calculating adult_diff4...")

df["adult_diff4"] = df.groupby(group_cols)["Adult_Total"].diff()

# Mark group boundaries
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)

df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)

df.loc[df["group_changed"], "adult_diff4"] = df.loc[df["group_changed"], "Adult_Total"]
df["adult_diff4"] = df["adult_diff4"].fillna(df["Adult_Total"])

df = df.drop(columns=[f"{col}_changed" for col in group_cols] + ["group_changed"])

# ============================================================
# STEP 3 — by_adult4
# ============================================================
print("🔹 Assigning by_adult4...")

by_adult4 = []
current = 1
prev_keys = None

for i, row in df.iterrows():
    keys = tuple(row[col] for col in group_cols)

    if keys != prev_keys:
        current = 1
        prev_keys = keys
    elif row["adult_diff4"] < 0 or row["day_diff4"] > 60:
        current += 1

    by_adult4.append(current)

df["by_adult4"] = by_adult4

# ============================================================
# STEP 4 — by_adult4_length
# ============================================================
print("🔹 Calculating by_adult4_length...")

lengths = (
    df.groupby(group_cols + ["by_adult4"])
    .size()
    .reset_index(name="by_adult4_length")
)

df = df.merge(lengths, on=group_cols + ["by_adult4"], how="left")

# ============================================================
# STEP 5 — by_short4
# ============================================================
print("🔹 Detecting short spillover runs (by_short4)...")

df["by_short4"] = ""

def detect_spill4(g):
    g = g.sort_values("date_iso").reset_index(drop=True)
    stock_type = str(g.loc[0, "Stock"]).strip().upper()

    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()
    runs = g[["by_adult4", "by_adult4_length"]].drop_duplicates().reset_index(drop=True)

    for i, r in runs.iterrows():
        curr_len = r["by_adult4_length"]

        if curr_len > 15:
            # Look ahead up to 4 runs
            for j in range(i + 1, min(len(runs), i + 5)):
                next_len = runs.loc[j, "by_adult4_length"]
                next_by = runs.loc[j, "by_adult4"]

                sub = g[g["by_adult4"] == next_by]

                if (sub["day_diff4"] > 250).any():
                    break

                # Skip tagging if this candidate is the final run for the identity
                if next_len < 5:
                    if j == len(runs) - 1:
                        continue
                    short_idx.update(sub.index)
                else:
                    break

    g.loc[g.index.isin(short_idx), "by_short4"] = "X"
    return g


df = (
    df.groupby(group_cols, group_keys=False)
      .apply(detect_spill4)
      .reset_index(drop=True)
)

# ============================================================
# STEP 6 — x_count4
# ============================================================
print("🔹 Counting contiguous X sequences (x_count4)...")

df["x_count4"] = 0

def count_x4(g):
    g = g.reset_index(drop=True)
    counts = [0] * len(g)

    i = 0
    while i < len(g):
        if g.loc[i, "by_short4"] == "X":
            j = i
            while j < len(g) and g.loc[j, "by_short4"] == "X":
                j += 1

            length = j - i
            for k in range(i, j):
                counts[k] = length

            i = j
        else:
            i += 1

    g["x_count4"] = counts
    return g


df = df.groupby(group_cols, group_keys=False).apply(count_x4).reset_index(drop=True)

# ============================================================
# SAVE BACK TO DB
# ============================================================
print("💾 Writing iteration4 metrics back to Escapement_PlotPipeline...")

df = reorder_for_output(df)

df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Iteration 4 Complete!")
print(f"📊 Rows processed: {len(df):,}")
print(f"📈 Short runs flagged: {(df['by_short4'] == 'X').sum():,}")
print(f"🔢 Max biological year (iteration4): {df['by_adult4'].max()}")
print("🏁 Biological metrics iteration 4 successfully updated.")
