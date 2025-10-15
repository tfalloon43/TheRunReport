# 17_counts.py
# ------------------------------------------------------------
# Expand "count_data" into 11 numeric columns (whole numbers, no decimals).
# Only active for rows with a date value.
# Rules:
#   - Fewer than 11 tokens → pad with NaN.
#   - More than 11 tokens → entire row set to NaN.
#   - Dash/missing/invalid → NaN.
#   - Rows with no date → all blank.
# Input  : csv_recent.csv
# Output : 17_counts_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import os
import pandas as pd
import numpy as np
import re

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "17_counts_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 17: Expanding count_data into detailed whole-number columns...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")
df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def parse_counts(row):
    """Parse count_data into 11 numeric values if date exists."""
    if not isinstance(row.get("date"), str) or not row["date"].strip():
        return [np.nan] * 11  # no date = leave blank

    value = row.get("count_data", "")
    if not isinstance(value, str) or not value.strip():
        return [np.nan] * 11

    tokens = re.split(r"\s+", value.strip())
    clean_vals = []

    for t in tokens:
        t = t.replace(",", "")
        if t in ("-", "--", "", " "):
            clean_vals.append(np.nan)
        else:
            try:
                clean_vals.append(int(t))
            except ValueError:
                clean_vals.append(np.nan)

    # Handle too few or too many tokens
    if len(clean_vals) < 11:
        clean_vals += [np.nan] * (11 - len(clean_vals))
    elif len(clean_vals) > 11:
        return [np.nan] * 11

    return clean_vals

# ------------------------------------------------------------
# Column names
# ------------------------------------------------------------
count_cols = [
    "Adult Total",
    "Jack Total",
    "Total Eggtake",
    "On Hand Adults",
    "On Hand Jacks",
    "Lethal Spawned",
    "Live Spawned",
    "Released",
    "Live Shipped",
    "Mortality",
    "Surplus",
]

# ------------------------------------------------------------
# Apply parsing
# ------------------------------------------------------------
expanded = df.apply(parse_counts, axis=1)
for i, col in enumerate(count_cols):
    df[col] = expanded.apply(lambda x: x[i])

# Convert numbers to whole numbers (no .0) when saving
def format_whole_number(val):
    if pd.isna(val):
        return ""
    try:
        return str(int(val))
    except:
        return ""

for col in count_cols:
    df[col] = df[col].apply(format_whole_number)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
filled = df["date"].astype(str).str.strip().ne("").sum()
print(f"✅ Count data expanded for {filled} rows with date values.")
print(f"🔄 csv_recent.csv updated with 11 whole-number columns (no decimals).")
print(f"💾 Output saved → {output_path}")