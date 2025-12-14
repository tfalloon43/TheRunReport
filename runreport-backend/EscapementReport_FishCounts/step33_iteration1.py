# step33_iteration1.py
# ------------------------------------------------------------
# Step 33: Biological Metrics Iteration 1
#
# Recomputes the following **inside the database table**:
#
#   day_diff
#   adult_diff
#   by_adult
#   by_adult_length
#   by_short
#   x_count
#
# All computations use the live table ESCAPEMENT_PLOTPipeline.
# No CSVs involved.
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


print("🏗️ Step 33: Recomputing biological metrics (Iteration 1)...")

# ------------------------------------------------------------
# DB PATH
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "0_db" / "local.db"

print(f"🗄️ Using DB → {db_path}")

# ------------------------------------------------------------
# Load DB table
# ------------------------------------------------------------
conn = sqlite3.connect(db_path)

df = pd.read_sql_query("SELECT * FROM Escapement_PlotPipeline;", conn)
print(f"✅ Loaded {len(df):,} rows from Escapement_PlotPipeline")

# ------------------------------------------------------------
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

# REQUIRED COLUMN CHECK (underscore schema)
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
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# NORMALIZE TYPES
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# SORT
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ============================================================
# STEP 1 — day_diff
# ============================================================
print("🔹 Calculating day_diff...")

df["day_diff"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

# ============================================================
# STEP 2 — adult_diff
# ============================================================
print("🔹 Calculating adult_diff...")

df["adult_diff"] = df.groupby(group_cols)["Adult_Total"].diff()

# Mark group boundaries
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)

df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)

# Reset adult_diff for new groups
df.loc[df["group_changed"], "adult_diff"] = df.loc[df["group_changed"], "Adult_Total"]

df["adult_diff"] = df["adult_diff"].fillna(df["Adult_Total"])

# Remove temp cols
df = df.drop(columns=[f"{col}_changed" for col in group_cols] + ["group_changed"])

# ============================================================
# STEP 3 — by_adult
# ============================================================
print("🔹 Assigning by_adult...")

by_adult = []
current_year = 1
prev_keys = None

for i, row in df.iterrows():
    keys = tuple(row[col] for col in group_cols)

    if keys != prev_keys:
        current_year = 1
        prev_keys = keys
    elif row["adult_diff"] < 0 or row["day_diff"] > 60:
        current_year += 1

    by_adult.append(current_year)

df["by_adult"] = by_adult

# ============================================================
# STEP 4 — by_adult_length
# ============================================================
print("🔹 Calculating by_adult_length...")

lengths = (
    df.groupby(group_cols + ["by_adult"])
    .size()
    .reset_index(name="by_adult_length")
)

df = df.merge(lengths, on=group_cols + ["by_adult"], how="left")

# ============================================================
# STEP 5 — by_short
# ============================================================
print("🔹 Detecting spillover short runs (by_short)...")

df["by_short"] = ""

def detect_spillover(g):
    g = g.sort_values("date_iso").reset_index(drop=True)
    stock_type = str(g.loc[0, "Stock"]).strip().upper()

    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()

    runs = g[["by_adult", "by_adult_length"]].drop_duplicates().reset_index(drop=True)

    for i, row in runs.iterrows():
        curr_len = row["by_adult_length"]

        if curr_len > 15:
            lookahead = runs.iloc[i+1:i+5]

            for _, nxt in lookahead.iterrows():
                next_len = nxt["by_adult_length"]
                next_by = nxt["by_adult"]

                sub = g[g["by_adult"] == next_by]

                if (sub["day_diff"] > 250).any():
                    break

                if next_len < 5:
                    short_idx.update(sub.index)
                else:
                    break

    g.loc[g.index.isin(short_idx), "by_short"] = "X"
    return g

df = (
    df.groupby(group_cols, group_keys=False)
      .apply(detect_spillover)
      .reset_index(drop=True)
)

# ============================================================
# STEP 6 — x_count
# ============================================================
print("🔹 Counting contiguous X sequences (x_count)...")

df["x_count"] = 0

def count_x(g):
    g = g.reset_index(drop=True)
    counts = [0] * len(g)

    i = 0
    while i < len(g):
        if g.loc[i, "by_short"] == "X":
            j = i
            while j < len(g) and g.loc[j, "by_short"] == "X":
                j += 1

            length = j - i
            for k in range(i, j):
                counts[k] = length

            i = j
        else:
            i += 1

    g["x_count"] = counts
    return g

df = df.groupby(group_cols, group_keys=False).apply(count_x).reset_index(drop=True)

# ============================================================
# SAVE BACK TO DATABASE
# ============================================================
print("💾 Writing metrics back to Escapement_PlotPipeline...")

df = reorder_for_output(df)

df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)

conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Iteration 1 Complete!")
print(f"📊 Rows processed: {len(df):,}")
print(f"📈 Short runs flagged: {(df['by_short'] == 'X').sum():,}")
print(f"🔢 Max biological year: {df['by_adult'].max()}")
print("🏁 Escapement biological metrics successfully updated.")
