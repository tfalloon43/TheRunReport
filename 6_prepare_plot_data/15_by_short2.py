# 15_by_short2.py
# ------------------------------------------------------------
# Step 15: Identify short "spillover" runs (<5 length)
# that follow a long run (>15 length) using the by_adult2 system.
#
# Biological intent:
#   Detect weird end effects where one long season ends and
#   several small runs (often early next-season counts) appear
#   in the same reporting stream.
#
# Logic:
#   - Applies when Stock ∈ ["H", "W", "U"]
#   - Group by facility/species/Stock/Stock_BO
#   - For each long run (by_adult2_length > 15):
#       → Look ahead through the next 4 by_adult2 transitions
#       → If a run has by_adult2_length < 5 and day_diff2 ≤ 250,
#         mark all rows in that run as 'X'
#       → Stop scanning when a run has by_adult2_length ≥ 5
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/15_by_short2_output.csv + updates csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 15: Detecting spillover short runs (based on by_adult2)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "15_by_short2_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
required_cols = [
    "facility", "species", "Stock", "Stock_BO",
    "by_adult2", "by_adult2_length", "day_diff2"
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Initialize new column
# ------------------------------------------------------------
df["by_short2"] = ""

# ------------------------------------------------------------
# Group-level function
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]

def flag_spillover_short_runs(g):
    """Flag short runs after long by_adult2 sequences within the same biological identity."""
    g = g.sort_values("date_iso").reset_index(drop=True)

    # Skip groups not H/W stocks
    stock_type = str(g.loc[0, "Stock"]).strip().upper()
    if stock_type not in ["H", "W", "U"]:
        return g

    short_idx = set()

    # Get distinct runs within this group
    runs = g[["by_adult2", "by_adult2_length"]].drop_duplicates().reset_index(drop=True)

    for i, row in runs.iterrows():
        curr_by = row["by_adult2"]
        curr_len = row["by_adult2_length"]

        # Only look ahead if this is a "long" biological run
        if curr_len > 15:
            lookahead_runs = runs.iloc[i+1:i+5]  # up to 4 subsequent runs

            for _, next_row in lookahead_runs.iterrows():
                next_by = next_row["by_adult2"]
                next_len = next_row["by_adult2_length"]

                # Get subset of rows for that run
                sub = g[g["by_adult2"] == next_by]

                # Stop if there’s a large gap in time (new season likely)
                if (sub["day_diff2"] > 250).any():
                    break

                # Mark short runs
                if next_len < 5:
                    short_idx.update(sub.index)
                else:
                    break  # stop once a normal-length run is found

    # Mark flagged rows
    g.loc[g.index.isin(short_idx), "by_short2"] = "X"
    return g

# ------------------------------------------------------------
# Apply to each biological group
# ------------------------------------------------------------
df = (
    df.groupby(group_cols, group_keys=False)
      .apply(flag_spillover_short_runs)
      .reset_index(drop=True)
)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
num_flagged = (df["by_short2"] == "X").sum()
print(f"✅ Spillover detection (by_adult2) complete → {output_path}")
print(f"🔎 {num_flagged:,} rows flagged as short (<5) runs following long seasons (>15).")
