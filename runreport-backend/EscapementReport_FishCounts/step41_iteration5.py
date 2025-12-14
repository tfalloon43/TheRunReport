# step41_iteration5.py
# ------------------------------------------------------------
# Step 41: Biological Metrics Iteration 5 (DB Version)
#
# Recomputes the following directly inside the database:
#   day_diff5
#   adult_diff5
#   by_adult5
#   by_adult5_length
#   by_short5   (ONLY for salmonid families)
#   x_count5
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


print("🏗️ Step 41: Recomputing biological metrics (Iteration 5)...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"
print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# LOAD FROM DB
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
    "Family",
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

# Stable sort: ensures identical-date rows maintain old order
df = df.reset_index().sort_values(group_cols + ["date_iso", "index"]).reset_index(drop=True)

# ============================================================
# STEP 1 — day_diff5
# ============================================================
print("🔹 Calculating day_diff5...")

df["day_diff5"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

# ============================================================
# STEP 2 — adult_diff5
# ============================================================
print("🔹 Calculating adult_diff5...")

df["adult_diff5"] = df.groupby(group_cols)["Adult_Total"].diff()

for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)

df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)

df.loc[df["group_changed"], "adult_diff5"] = df.loc[df["group_changed"], "Adult_Total"]
df["adult_diff5"] = df["adult_diff5"].fillna(df["Adult_Total"])

df = df.drop(columns=[f"{col}_changed" for col in group_cols] + ["group_changed"])

# ============================================================
# STEP 3 — by_adult5
# ============================================================
print("🔹 Assigning by_adult5...")

by_adult5 = []
current = 1
prev_keys = None

for i, row in df.iterrows():
    keys = tuple(row[col] for col in group_cols)

    if keys != prev_keys:
        current = 1
        prev_keys = keys
    elif row["adult_diff5"] < 0 or row["day_diff5"] > 60:
        current += 1

    by_adult5.append(current)

df["by_adult5"] = by_adult5

# ============================================================
# STEP 4 — by_adult5_length
# ============================================================
print("🔹 Calculating by_adult5_length...")

lengths = (
    df.groupby(group_cols + ["by_adult5"])
    .size()
    .reset_index(name="by_adult5_length")
)

df = df.merge(lengths, on=group_cols + ["by_adult5"], how="left")

# ============================================================
# STEP 5 — by_short5 (SALMONIDS ONLY)
# ============================================================
print("🔹 Detecting short spillover runs (by_short5, salmonid families only)...")

df["by_short5"] = ""

valid_families = ["Steelhead", "Chinook", "Coho", "Chum", "Pink", "Sockeye"]

def detect_spill5(g):
    g = g.sort_values("date_iso").reset_index(drop=True)

    family = str(g.loc[0, "Family"]).strip().title()
    if family not in valid_families:
        return g  # skip non-salmonids entirely

    stock_type = str(g.loc[0, "Stock"]).strip().upper()
    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()
    runs = g[["by_adult5", "by_adult5_length"]].drop_duplicates().reset_index(drop=True)

    for i, r in runs.iterrows():
        if r["by_adult5_length"] > 15:
            # Look ahead up to 4 runs
            for j in range(i + 1, min(len(runs), i + 5)):
                next_len = runs.loc[j, "by_adult5_length"]
                next_by = runs.loc[j, "by_adult5"]

                sub = g[g["by_adult5"] == next_by]

                if (sub["day_diff5"] > 250).any():
                    break

                # Skip tagging if this candidate is the final run for the identity
                if next_len < 5:
                    if j == len(runs) - 1:
                        continue
                    short_idx.update(sub.index)
                else:
                    break

    g.loc[g.index.isin(short_idx), "by_short5"] = "X"
    return g


df = df.groupby(group_cols, group_keys=False).apply(detect_spill5).reset_index(drop=True)

# ============================================================
# STEP 6 — x_count5
# ============================================================
print("🔹 Counting contiguous X sequences (x_count5)...")

df["x_count5"] = 0

def count_x5(g):
    g = g.reset_index(drop=True)
    counts = [0] * len(g)
    i = 0

    while i < len(g):
        if g.loc[i, "by_short5"] == "X":
            j = i
            while j < len(g) and g.loc[j, "by_short5"] == "X":
                j += 1

            length = j - i
            for k in range(i, j):
                counts[k] = length
            i = j
        else:
            i += 1

    g["x_count5"] = counts
    return g


df = df.groupby(group_cols, group_keys=False).apply(count_x5).reset_index(drop=True)

# ============================================================
# SAVE BACK TO DB
# ============================================================
print("💾 Saving iteration5 metrics back to database...")

df = reorder_for_output(df)

df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Iteration 5 Complete!")
print(f"📊 Rows processed: {len(df):,}")
print(f"🐟 Salmonid short-run flags applied: {(df['by_short5'] == 'X').sum():,}")
print(f"🔢 Max biological year: {df['by_adult5'].max()}")
print("🏁 Biological metrics iteration 5 successfully updated.")
