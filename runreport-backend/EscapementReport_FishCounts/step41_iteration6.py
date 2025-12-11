# step41_iteration6.py
# ------------------------------------------------------------
# Step 41: Biological Metrics Iteration 6 (DB Version)
#
# Recomputes the following inside the database:
#   day_diff6
#   adult_diff6
#   by_adult6
#   by_adult6_length
#   by_short6  (salmonids only)
#   x_count6
#
# Reads + rewrites Escapement_PlotPipeline.
# ------------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path

print("🏗️ Step 41: Recomputing biological metrics (Iteration 6)...")

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

# Stable sort (preserve row order inside identical timestamps)
df = df.reset_index().sort_values(group_cols + ["date_iso", "index"]).reset_index(drop=True)

# ============================================================
# STEP 1 — day_diff6 and adult_diff6
# ============================================================
print("🔹 Calculating day_diff6 and adult_diff6...")

df["day_diff6"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

df["adult_diff6"] = df.groupby(group_cols)["Adult_Total"].diff()

# Reset diffs when group changes
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)

df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)

df.loc[df["group_changed"], "adult_diff6"] = df.loc[df["group_changed"], "Adult_Total"]
df["adult_diff6"] = df["adult_diff6"].fillna(df["Adult_Total"])

df = df.drop(columns=[f"{col}_changed" for col in group_cols] + ["group_changed"])

# ============================================================
# STEP 2 — by_adult6
# ============================================================
print("🔹 Assigning by_adult6...")

by_adult6 = []
current = 1
prev_keys = None

for i, row in df.iterrows():
    keys = tuple(row[col] for col in group_cols)

    if keys != prev_keys:
        current = 1
        prev_keys = keys
    elif row["adult_diff6"] < 0 or row["day_diff6"] > 60:
        current += 1

    by_adult6.append(current)

df["by_adult6"] = by_adult6

# ============================================================
# STEP 3 — by_adult6_length
# ============================================================
print("🔹 Calculating by_adult6_length...")

lengths = (
    df.groupby(group_cols + ["by_adult6"])
    .size()
    .reset_index(name="by_adult6_length")
)

df = df.merge(lengths, on=group_cols + ["by_adult6"], how="left")

# ============================================================
# STEP 4 — by_short6 (SALMONIDS ONLY)
# ============================================================
print("🔹 Detecting short spillover runs (by_short6, salmonids only)...")

df["by_short6"] = ""

valid_families = ["Steelhead", "Chinook", "Coho", "Chum", "Pink", "Sockeye"]

def detect_spill6(g):
    g = g.sort_values("date_iso").reset_index(drop=True)

    family = str(g.loc[0, "Family"]).strip().title()
    if family not in valid_families:
        return g  # Not a salmonid → skip

    stock_type = str(g.loc[0, "Stock"]).strip().upper()
    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()
    runs = g[["by_adult6", "by_adult6_length"]].drop_duplicates().reset_index(drop=True)

    for i, r in runs.iterrows():
        curr_len = r["by_adult6_length"]

        if curr_len > 15:
            lookahead = runs.iloc[i+1:i+5]

            for _, nxt in lookahead.iterrows():
                next_len = nxt["by_adult6_length"]
                next_by = nxt["by_adult6"]

                sub = g[g["by_adult6"] == next_by]

                # long break between seasons → stop scanning
                if (sub["day_diff6"] > 250).any():
                    break

                if next_len < 5:
                    short_idx.update(sub.index)
                else:
                    break

    g.loc[g.index.isin(short_idx), "by_short6"] = "X"
    return g

df = df.groupby(group_cols, group_keys=False).apply(detect_spill6).reset_index(drop=True)

# ============================================================
# STEP 5 — x_count6
# ============================================================
print("🔹 Counting contiguous X sequences (x_count6)...")

df["x_count6"] = 0

def count_x6(g):
    g = g.sort_values(["date_iso", "index"]).reset_index(drop=True)
    counts = [0] * len(g)

    i = 0
    while i < len(g):
        if g.loc[i, "by_short6"] == "X":
            j = i
            while j < len(g) and g.loc[j, "by_short6"] == "X":
                j += 1

            length = j - i
            for k in range(i, j):
                counts[k] = length
            i = j
        else:
            i += 1

    g["x_count6"] = counts
    return g

df = (
    df.groupby(group_cols + ["by_adult6"], group_keys=False)
      .apply(count_x6)
      .reset_index(drop=True)
)

# ============================================================
# FINAL SORT & SAVE
# ============================================================
df = df.sort_values(group_cols + ["by_adult6", "date_iso", "index"]).reset_index(drop=True)

print("💾 Writing iteration6 metrics back to Escapement_PlotPipeline...")
df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)

conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Iteration 6 Complete!")
print(f"📊 Rows processed: {len(df):,}")
print(f"🐟 Salmonid short-run flags: {(df['by_short6'] == 'X').sum():,}")
print(f"🔢 Max biological year (v6): {df['by_adult6'].max()}")
print("🏁 Biological metrics iteration 6 successfully updated.")
