# 22_iteration5.py
# ------------------------------------------------------------
# Step 22: Full re-iteration of biological year metrics (v5)
#
# After additional row removals and cleanup (x4 condense),
# this script recalculates:
#   ✅ day_diff5
#   ✅ adult_diff5
#   ✅ by_adult5
#   ✅ by_adult5_length
#   ✅ by_short5
#   ✅ x_count5
#
# Logic (summarized):
#   - Work within biological identity groups:
#         facility, species, Stock, Stock_BO
#   - Recalculate diffs, biological years, and short-run flags
#     using the most up-to-date cleaned data.
#   - STEP 4 (by_short5) runs only when Family ∈
#     ["Steelhead", "Chinook", "Coho", "Chum", "Pink", "Sockeye"]
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/22_iteration5_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 22: Rebuilding biological metrics (day_diff5 → x_count5)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "22_iteration5_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Validate core columns
# ------------------------------------------------------------
required_cols = ["facility", "species", "Stock", "Stock_BO", "Family", "date_iso", "Adult_Total"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Normalize types
# ------------------------------------------------------------
df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0)

group_cols = ["facility", "species", "Stock", "Stock_BO"]

# ✅ NEW: stable sort so identical-date rows preserve their sequence order
df = df.reset_index().sort_values(group_cols + ["date_iso", "index"]).reset_index(drop=True)

# ============================================================
# STEP 1: day_diff5 and adult_diff5
# ============================================================
print("🔹 Calculating day_diff5 and adult_diff5...")
df["day_diff5"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

# normal diff, negatives retained
df["adult_diff5"] = df.groupby(group_cols)["Adult_Total"].diff()

# reset when group changes
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)
df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)
df.loc[df["group_changed"], "adult_diff5"] = df.loc[df["group_changed"], "Adult_Total"]
df["adult_diff5"] = df["adult_diff5"].fillna(df["Adult_Total"])

df = df.drop(columns=[f"{col}_changed" for col in group_cols] + ["group_changed"])

# ============================================================
# STEP 2: by_adult5 assignment
# ============================================================
print("🔹 Assigning by_adult5...")
by_adult5 = []
current_year = 1
prev_keys = None

for i, row in df.iterrows():
    keys = tuple(row[col] for col in group_cols)
    if keys != prev_keys:
        current_year = 1
        prev_keys = keys
    elif row["adult_diff5"] < 0 or row["day_diff5"] > 60:
        current_year += 1
    by_adult5.append(current_year)

df["by_adult5"] = by_adult5

# ============================================================
# STEP 3: by_adult5_length
# ============================================================
print("🔹 Calculating by_adult5_length...")
lengths = (
    df.groupby(group_cols + ["by_adult5"])
    .size()
    .reset_index(name="by_adult5_length")
)
df = df.merge(lengths, on=group_cols + ["by_adult5"], how="left")

# ============================================================
# STEP 4: by_short5 (spillover short run detection)
# ============================================================
print("🔹 Detecting short spillover runs (by_short5, salmonid families only)...")
df["by_short5"] = ""

valid_families = ["Steelhead", "Chinook", "Coho", "Chum", "Pink", "Sockeye"]

def flag_spillover_short_runs(g):
    g = g.sort_values("date_iso").reset_index(drop=True)

    # ✅ Only run if this group's Family is a salmonid
    family = str(g.loc[0, "Family"]).strip().title()
    if family not in valid_families:
        return g  # skip entire group if not salmonid

    stock_type = str(g.loc[0, "Stock"]).strip().upper()
    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()
    runs = g[["by_adult5", "by_adult5_length"]].drop_duplicates().reset_index(drop=True)

    for i, row in runs.iterrows():
        curr_len = row["by_adult5_length"]
        if curr_len > 15:
            lookahead = runs.iloc[i+1:i+5]
            for _, next_row in lookahead.iterrows():
                next_len = next_row["by_adult5_length"]
                next_by = next_row["by_adult5"]
                sub = g[g["by_adult5"] == next_by]
                if (sub["day_diff5"] > 250).any():
                    break
                if next_len < 5:
                    short_idx.update(sub.index)
                else:
                    break

    g.loc[g.index.isin(short_idx), "by_short5"] = "X"
    return g

df = (
    df.groupby(group_cols, group_keys=False)
      .apply(flag_spillover_short_runs)
      .reset_index(drop=True)
)

# ============================================================
# STEP 5: x_count5 (contiguous X sequence count)
# ============================================================
print("🔹 Counting contiguous X sequences (x_count5)...")
df["x_count5"] = 0

def count_x_sequences(g):
    g = g.reset_index(drop=True)
    counts = [0] * len(g)
    i = 0
    while i < len(g):
        if g.loc[i, "by_short5"] == "X":
            j = i
            while j < len(g) and g.loc[j, "by_short5"] == "X":
                j += 1
            group_len = j - i
            for k in range(i, j):
                counts[k] = group_len
            i = j
        else:
            i += 1
    g["x_count5"] = counts
    return g

df = df.groupby(group_cols, group_keys=False).apply(count_x_sequences).reset_index(drop=True)

# ============================================================
# SAVE OUTPUTS
# ============================================================
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Iteration 5 complete → {output_path}")
print(f"📊 Recomputed columns: day_diff5, adult_diff5, by_adult5, by_adult5_length, by_short5, x_count5")
print(f"🔢 Total rows: {len(df):,}")
print(f"🔎 Short runs flagged (salmonids only): {(df['by_short5'] == 'X').sum():,}")
