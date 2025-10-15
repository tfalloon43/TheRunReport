# 10_TL6.py
# ------------------------------------------------------------
# Build TL6 column
# Conditions:
#   • Only active if stock_presence_lower has a value.
#   • TL6 starts from TL5, keeping everything up to and including
#     the first stock indicator (H/W/U/M/C).
#   • Cleans out leading words (e.g., 'WEIR', 'HATCHERY', 'TRAP')
#     so that the text begins directly with 'Stock-' or 'River-'.
#   • Case sensitive — 'HATCHERY' ≠ 'Hatchery'.
# Input  : csv_recent.csv
# Output : 10_TL6_output.csv + csv_recent.csv (updated snapshot)
# ------------------------------------------------------------

import os
import pandas as pd
import re

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "10_TL6_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 10: Creating TL6 (case-sensitive cleanup)...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")
df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Editable cleanup list (case-sensitive)
# ------------------------------------------------------------
cleanup_before_stock = [
    "WYNOOCHEE R DAM ", 
    "WEIR ", 
    "HATCHERY ",
    "",
    "",
    "",
    "",
    "",
]

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def make_TL6(tl5, stock_presence_lower):
    """
    Build TL6:
    - If stock_presence_lower is empty → return blank.
    - Otherwise, copy TL5 but:
        1. Keep only up to and including the first stock indicator (H/W/U/M/C).
        2. If a known cleanup word appears before 'Stock-' or 'River-', remove it (case-sensitive).
    """
    if not isinstance(stock_presence_lower, str) or not stock_presence_lower.strip():
        return ""
    if not isinstance(tl5, str) or not tl5.strip():
        return ""

    # Step 1: Trim TL5 down to everything up to and including the first stock indicator
    match = re.search(r"\b([HWUMC])\b", tl5)
    if match:
        end_pos = match.end()
        tl6 = tl5[:end_pos].strip()
    else:
        tl6 = tl5.strip()

    # Step 2: Clean leading junk before 'Stock-' or 'River-' if cleanup keywords exist
    cleanup_pattern = r"^(?:\b(?:" + "|".join(cleanup_before_stock) + r")\b\s*)+(?=(Stock-|River-))"
    tl6 = re.sub(cleanup_pattern, "", tl6).strip()  # ❌ no IGNORECASE flag

    # Step 3: If TL6 contains something like 'X Stock-' (X = junk word not in list), remove everything before Stock-
    if re.search(r"\b(Stock-|River-)", tl6):
        tl6 = re.sub(r"^.*?\b(Stock-|River-)", r"\1", tl6).strip()

    return tl6

# ------------------------------------------------------------
# Apply logic
# ------------------------------------------------------------
df["TL6"] = df.apply(
    lambda r: make_TL6(r.get("TL5", ""), r.get("stock_presence_lower", "")), axis=1
)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
total_rows = len(df)
populated = df["TL6"].astype(str).str.strip().ne("").sum()
print(f"✅ TL6 extraction complete → {output_path}")
print(f"🔄 csv_recent.csv updated with TL6 column")
print(f"📊 {populated} of {total_rows} rows populated with TL6 (case-sensitive)")