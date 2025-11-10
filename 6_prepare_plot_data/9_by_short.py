# 9_by_short_v2.py
# ------------------------------------------------------------
# Step 9: Identify short "spillover" runs (<5 length)
# that follow a long run (>15 length) within the same group.
#
# Biological intent:
#   - Detect weird end effects where one long season ends and
#     several small runs (often early next-season counts) appear
#     in the same report stream.
#
# Logic:
#   - Only applies when Stock ∈ ["H", "W"]
#   - Group by facility/species/Stock/Stock_BO
#   - For each long run (by_adult_length > 15):
#       → Look ahead through the next 4 by_adult transitions
#       → If a run has by_adult_length < 5 and day_diff ≤ 250,
#         mark all rows in that run as 'X'
#       → Stop scanning when a run has by_adult_length ≥ 5
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/9_by_short_v2_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 9 (v2): Detecting spillover short runs after long seasons...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "9_by_short_v2_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Validate columns
# ------------------------------------------------------------
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "by_adult", "by_adult_length", "day_diff"
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Initialize column
# ------------------------------------------------------------
df["by_short"] = ""

# ------------------------------------------------------------
# Define grouping
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]

def flag_spillover_short_runs(g):
    g = g.sort_values("date_iso").reset_index(drop=True)

    # Skip if stock type not H or W
    stock_type = str(g.loc[0, "Stock"]).strip().upper()
    if stock_type not in ["H", "W"]:
        return g

    short_idx = set()

    # Identify distinct runs
    runs = g[["by_adult", "by_adult_length"]].drop_duplicates().reset_index(drop=True)

    for i, row in runs.iterrows():
        curr_by = row["by_adult"]
        curr_len = row["by_adult_length"]

        # Only trigger lookahead if current run is long
        if curr_len > 15:
            lookahead_runs = runs.iloc[i+1:i+5]  # next up to 4 runs

            for _, next_row in lookahead_runs.iterrows():
                next_by = next_row["by_adult"]
                next_len = next_row["by_adult_length"]

                # Get rows belonging to this next run
                sub = g[g["by_adult"] == next_by]

                # Skip if any of these rows have a large day_diff (likely a new season)
                if (sub["day_diff"] > 250).any():
                    break  # stop scanning forward

                # If short, mark all rows
                if next_len < 5:
                    short_idx.update(sub.index)
                else:
                    break  # stop at first non-short run

    # Flag all marked rows
    g.loc[g.index.isin(short_idx), "by_short"] = "X"
    return g


# ------------------------------------------------------------
# Apply to all hatchery/species/stock groups
# ------------------------------------------------------------
df = (
    df.groupby(group_cols, group_keys=False)
      .apply(flag_spillover_short_runs)
      .reset_index(drop=True)
)

# ------------------------------------------------------------
# Save
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

num_flagged = (df["by_short"] == "X").sum()
print(f"✅ Spillover detection complete → {output_path}")
print(f"🔎 {num_flagged:,} rows flagged as short (<5) runs following long seasons (>15).")