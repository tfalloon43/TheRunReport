# 9_by_short.py
# ------------------------------------------------------------
# Step 9: Identify and flag short biological runs (<5 length)
# that follow a change in by_adult within the same group.
#
# Logic:
#   - Only applies when Stock ∈ ["H", "W"]
#   - When by_adult changes within the same facility/species/Stock/Stock_BO,
#     check the next row's by_adult_length.
#   - If that next value < 5, look ahead up to 5 rows.
#   - Mark every row in that lookahead window with by_adult_length < 5 as 'X',
#     UNLESS day_diff > 60 (large time gap, likely new season)
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/9_by_short_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 9: Identifying short biological runs after by_adult change (H/W only, excluding large gaps)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "9_by_short_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Verify required columns
# ------------------------------------------------------------
required_cols = [
    "facility",
    "species",
    "Stock",
    "Stock_BO",
    "by_adult",
    "by_adult_length",
    "day_diff",
]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns: {missing}")

# ------------------------------------------------------------
# Initialize column
# ------------------------------------------------------------
df["by_short"] = ""

# ------------------------------------------------------------
# Grouped logic
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]

def flag_short_rows(g):
    """Flag short sequences only for H or W stock groups, ignoring large day gaps."""
    g = g.reset_index(drop=True)

    # Skip this group if Stock is not H or W
    stock_type = str(g.loc[0, "Stock"]).strip().upper()
    if stock_type not in ["H", "W"]:
        return g  # leave by_short blank

    n = len(g)
    short_idx = set()

    for i in range(1, n):  # start at 1 so we can compare to previous
        # Detect a by_adult transition
        if g.loc[i, "by_adult"] != g.loc[i - 1, "by_adult"]:
            # Look ahead up to 5 rows
            lookahead_end = min(i + 6, n)
            next_len = g.loc[i, "by_adult_length"]

            # Only proceed if next length is short (<5)
            if next_len < 5:
                for j in range(i, lookahead_end):
                    if (
                        g.loc[j, "by_adult_length"] < 4
                        and g.loc[j, "day_diff"] <= 250  # 🚫 skip large time gaps
                    ):
                        short_idx.add(j)

    # Mark flagged rows
    g.loc[g.index.isin(short_idx), "by_short"] = "X"
    return g

# ------------------------------------------------------------
# Apply per hatchery/species/stock group
# ------------------------------------------------------------
df = (
    df.groupby(group_cols, group_keys=False)
    .apply(flag_short_rows)
    .reset_index(drop=True)
)

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

num_flagged = (df["by_short"] == "X").sum()
print(f"✅ Flagging complete → {output_path}")
print(f"🔎 {num_flagged:,} rows marked as short (<5) runs following a by_adult change (H/W only, no gaps >60 days).")