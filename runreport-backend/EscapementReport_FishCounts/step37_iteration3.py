# step37_iteration3.py
# ------------------------------------------------------------
# Step 37: Biological Metrics Iteration 3
#
# Recomputes the following **inside the database**:
#
#   day_diff3
#   adult_diff3
#   by_adult3
#   by_adult3_length
#   by_short3
#   x_count3
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


print("🏗️ Step 37: Recomputing biological metrics (Iteration 3)...")

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
# STEP 1 — day_diff3
# ============================================================
print("🔹 Calculating day_diff3...")

df["day_diff3"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

# ============================================================
# STEP 2 — adult_diff3
# ============================================================
print("🔹 Calculating adult_diff3...")

df["adult_diff3"] = df.groupby(group_cols)["Adult_Total"].diff()

# Reset adult_diff3 for new groups (first row in each group)
first_in_group = df.groupby(group_cols).cumcount() == 0
df.loc[first_in_group, "adult_diff3"] = df.loc[first_in_group, "Adult_Total"]
df["adult_diff3"] = df["adult_diff3"].fillna(df["Adult_Total"])

# ============================================================
# STEP 3 — by_adult3
# ============================================================
print("🔹 Assigning by_adult3...")

trigger = (df["adult_diff3"] < 0) | (df["day_diff3"] > 90)
trigger = trigger & ~first_in_group
df["by_adult3"] = trigger.groupby([df[c] for c in group_cols]).cumsum() + 1

# ============================================================
# STEP 4 — by_adult3_length
# ============================================================
print("🔹 Calculating by_adult3_length...")

lengths = (
    df.groupby(group_cols + ["by_adult3"])
    .size()
    .reset_index(name="by_adult3_length")
)

df = df.merge(lengths, on=group_cols + ["by_adult3"], how="left")

# ============================================================
# STEP 5 — by_short3
# ============================================================
print("🔹 Detecting short spillover runs (by_short3)...")

df["by_short3"] = ""

def detect_spill3(g):
    g = g.reset_index(drop=True)
    stock_type = str(g.loc[0, "Stock"]).strip().upper()

    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()
    runs = g[["by_adult3", "by_adult3_length"]].drop_duplicates().reset_index(drop=True)

    for i, r in runs.iterrows():
        curr_len = r["by_adult3_length"]

        if curr_len > 15:
            # Look ahead up to 4 runs
            for j in range(i + 1, min(len(runs), i + 5)):
                next_len = runs.loc[j, "by_adult3_length"]
                next_by = runs.loc[j, "by_adult3"]

                sub = g[g["by_adult3"] == next_by]

                # If there's a long gap, stop looking further
                if (sub["day_diff3"] > 250).any():
                    break

                # Skip tagging if this candidate is the final run for the identity
                if next_len < 5:
                    if j == len(runs) - 1:
                        continue
                    short_idx.update(sub.index)
                else:
                    break

    g.loc[g.index.isin(short_idx), "by_short3"] = "X"
    return g

df = (
    df.groupby(group_cols, group_keys=False)
      .apply(detect_spill3)
      .reset_index(drop=True)
)

# ============================================================
# STEP 6 — x_count3
# ============================================================
print("🔹 Counting contiguous X sequences (x_count3)...")

mask = df["by_short3"] == "X"
group_boundary = df[group_cols].ne(df[group_cols].shift()).any(axis=1)
run_id = (mask.ne(mask.shift()) | group_boundary).cumsum()
run_len = df.groupby(run_id)["by_short3"].transform("size")
df["x_count3"] = run_len.where(mask, 0).astype(int)

# ============================================================
# SAVE BACK TO DB
# ============================================================
print("💾 Writing iteration3 metrics back to Escapement_PlotPipeline...")

df = reorder_for_output(df)

df.to_sql("Escapement_PlotPipeline", conn, if_exists="replace", index=False)
conn.close()

# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------
print("✅ Iteration 3 Complete!")
print(f"📊 Rows processed: {len(df):,}")
print(f"📈 Short runs flagged: {(df['by_short3'] == 'X').sum():,}")
print(f"🔢 Max biological year (iteration3): {df['by_adult3'].max()}")
print("🏁 Biological metrics iteration 3 successfully updated.")
