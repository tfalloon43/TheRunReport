# 10_Xcount.py
# ------------------------------------------------------------
# Step 10: Count contiguous 'X' sequences in by_short
#
# Adds a column 'x_count' that counts how many consecutive
# 'X' values appear in a run within each facility/species/Stock/Stock_BO group.
#
# Example:
# X, X, _, X, X, X → 2, 2, 0, 3, 3, 3
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/10_Xcount_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path

print("🏗️ Step 10: Counting contiguous X sequences in by_short...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "10_Xcount_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")
df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Ensure column exists
# ------------------------------------------------------------
if "by_short" not in df.columns:
    raise ValueError("❌ Missing 'by_short' column. Run 9_by_short.py first.")

# ------------------------------------------------------------
# Initialize new column
# ------------------------------------------------------------
df["x_count"] = 0

# ------------------------------------------------------------
# Define function per group
# ------------------------------------------------------------
group_cols = ["facility", "species", "Stock", "Stock_BO"]

def count_x_sequences(g):
    g = g.reset_index(drop=True)
    n = len(g)
    counts = [0] * n

    i = 0
    while i < n:
        if g.loc[i, "by_short"] == "X":
            # Count how many consecutive X's
            j = i
            while j < n and g.loc[j, "by_short"] == "X":
                j += 1
            group_len = j - i
            # Assign that count to all rows in the sequence
            for k in range(i, j):
                counts[k] = group_len
            i = j  # skip ahead
        else:
            i += 1

    g["x_count"] = counts
    return g

# ------------------------------------------------------------
# Apply per hatchery/species group
# ------------------------------------------------------------
df = df.groupby(group_cols, group_keys=False).apply(count_x_sequences).reset_index(drop=True)

# ------------------------------------------------------------
# Save results
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

num_flagged = (df["x_count"] > 0).sum()
print(f"✅ x_count calculation complete → {output_path}")
print(f"🔢 {num_flagged:,} rows marked as part of contiguous 'X' sequences.")