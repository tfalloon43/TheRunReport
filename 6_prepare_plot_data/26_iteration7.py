# 26_iteration7.py
# ------------------------------------------------------------
# Step 26: Full re-iteration of biological year metrics (v7)
#
# After cleanup (v6), this script recalculates:
#   ✅ day_diff7
#   ✅ adult_diff7
#   ✅ by_adult7
#   ✅ by_adult7_length
#   ✅ by_short7
#   ✅ x_count7
#
# Logic (summarized):
#   - Work within biological identity groups:
#         facility, species, Stock, Stock_BO
#   - Recalculate diffs, biological years, and short-run flags
#     using the cleaned data.
#   - STEP 4 (by_short7) runs only when Family ∈
#     ["Steelhead", "Chinook", "Coho", "Chum", "Pink", "Sockeye"]
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/26_iteration7_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 26: Rebuilding biological metrics (day_diff7 → x_count7)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "26_iteration7_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Clean up any stray index columns (safety)
# ------------------------------------------------------------
for col in df.columns:
    if col.lower().startswith("unnamed") or col == "level_0":
        print(f"🧹 Dropping stray column: {col}")
        df = df.drop(columns=[col])

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

# ✅ Safe reset and stable sort
df = df.reset_index(drop=True)
if "index" not in df.columns:
    df = df.reset_index()
df = df.sort_values(group_cols + ["date_iso", "index"]).reset_index(drop=True)

# ============================================================
# STEP 1: day_diff7 and adult_diff7
# ============================================================
print("🔹 Calculating day_diff7 and adult_diff7...")
df["day_diff7"] = (
    df.groupby(group_cols)["date_iso"]
    .diff()
    .dt.days
    .fillna(7)
    .astype(int)
)

# retain negatives intentionally
df["adult_diff7"] = df.groupby(group_cols)["Adult_Total"].diff()

# reset when group changes
for col in group_cols:
    df[f"{col}_changed"] = df[col] != df[col].shift(1)
df["group_changed"] = df[[f"{col}_changed" for col in group_cols]].any(axis=1)
df.loc[df["group_changed"], "adult_diff7"] = df.loc[df["group_changed"], "Adult_Total"]
df["adult_diff7"] = df["adult_diff7"].fillna(df["Adult_Total"])

df = df.drop(columns=[f"{col}_changed" for col in group_cols] + ["group_changed"])

# ============================================================
# STEP 2: by_adult7 assignment
# ============================================================
# 🔧 ensure chronological order again before assigning biological years
df = df.sort_values(group_cols + ["date_iso", "index"]).reset_index(drop=True)

print("🔹 Assigning by_adult7...")
by_adult7 = []
current_year = 1
prev_keys = None

for i, row in df.iterrows():
    keys = tuple(row[col] for col in group_cols)
    if keys != prev_keys:
        current_year = 1
        prev_keys = keys
    elif row["adult_diff7"] < 0 or row["day_diff7"] > 60:
        current_year += 1
    by_adult7.append(current_year)

df["by_adult7"] = by_adult7

# ============================================================
# STEP 3: by_adult7_length
# ============================================================
print("🔹 Calculating by_adult7_length...")
lengths = (
    df.groupby(group_cols + ["by_adult7"])
    .size()
    .reset_index(name="by_adult7_length")
)
df = df.merge(lengths, on=group_cols + ["by_adult7"], how="left")

# ============================================================
# STEP 4: by_short7 (spillover short run detection)
# ============================================================
print("🔹 Detecting short spillover runs (by_short7, salmonid families only)...")
df["by_short7"] = ""

valid_families = ["Steelhead", "Chinook", "Coho", "Chum", "Pink", "Sockeye"]

def flag_spillover_short_runs(g):
    g = g.sort_values("date_iso").reset_index(drop=True)

    family = str(g.loc[0, "Family"]).strip().title()
    if family not in valid_families:
        return g  # skip if not salmonid

    stock_type = str(g.loc[0, "Stock"]).strip().upper()
    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()
    runs = g[["by_adult7", "by_adult7_length"]].drop_duplicates().reset_index(drop=True)

    for i, row in runs.iterrows():
        curr_len = row["by_adult7_length"]
        if curr_len > 15:
            lookahead = runs.iloc[i+1:i+5]
            for _, next_row in lookahead.iterrows():
                next_len = next_row["by_adult7_length"]
                next_by = next_row["by_adult7"]
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
      .apply(flag_spillover_short_runs)
      .reset_index(drop=True)
)

# ============================================================
# STEP 5: x_count7 (contiguous X sequence count)
# ============================================================
print("🔹 Counting contiguous X sequences (x_count7)...")
df["x_count7"] = 0

def count_x_sequences(g):
    g = g.sort_values(["date_iso", "index"]).reset_index(drop=True)
    counts = [0] * len(g)
    i = 0
    while i < len(g):
        if g.loc[i, "by_short7"] == "X":
            j = i
            while j < len(g) and g.loc[j, "by_short7"] == "X":
                j += 1
            group_len = j - i
            for k in range(i, j):
                counts[k] = group_len
            i = j
        else:
            i += 1
    g["x_count7"] = counts
    return g

df = (
    df.groupby(group_cols + ["by_adult7"], group_keys=False)
      .apply(count_x_sequences)
      .reset_index(drop=True)
)

# ============================================================
# FINAL ORDER & SAVE
# ============================================================
df = df.sort_values(group_cols + ["by_adult7", "date_iso", "index"]).reset_index(drop=True)

df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print(f"✅ Iteration 7 complete → {output_path}")
print(f"📊 Recomputed columns: day_diff7, adult_diff7, by_adult7, by_adult7_length, by_short7, x_count7")
print(f"🔢 Total rows: {len(df):,}")
print(f"🔎 Short runs flagged (salmonids only): {(df['by_short7'] == 'X').sum():,}")
