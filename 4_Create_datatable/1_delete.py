# 1_delete.py
# ------------------------------------------------------------
# Step 1: Remove any rows in csv_recent.csv where text_line
# contains the phrase "Final in-season estimate" (case-insensitive)
# AND drop the associated count rows to keep timelines clean.
#
# Association logic:
#   • If the "Final in-season estimate" line itself contains a
#     block of counts (e.g. "275 - - 10 - - - 263 - 2 -"), delete
#     that line as the count row.
#   • Otherwise, look UP to 5 rows above on the same pdf_name &
#     page_num for a line with a similar count pattern and delete
#     that row as well.
#
# Input  : 100_Data/csv_recent.csv
# Output : 100_Data/1_delete.csv  +  updates csv_recent.csv
# ------------------------------------------------------------

import pandas as pd
from pathlib import Path
import re

print("🧹 Step 1: Removing 'Final in-season estimate' lines and associated count rows...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"

input_path    = data_dir / "csv_recent.csv"
output_clean  = data_dir / "1_delete.csv"
output_recent = data_dir / "csv_recent.csv"   # overwrite

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"📂 Loaded {len(df):,} rows from csv_recent.csv")

# ------------------------------------------------------------
# Ensure required column exists
# ------------------------------------------------------------
if "text_line" not in df.columns:
    raise ValueError("❌ Column 'text_line' is missing in csv_recent.csv")

# ------------------------------------------------------------
# Phrase + count pattern detection
# ------------------------------------------------------------
phrase = "final in-season estimate"

# "Count row" = at least 5 tokens that are either a number (possibly with commas)
# or a dash, separated by spaces. This matches lines like:
#   "275 - - 10 - - - 263 - 2 -"
count_pattern = re.compile(
    r'(?:\d{1,3}(?:,\d{3})?|-)(?:\s+(?:\d{1,3}(?:,\d{3})?|-)){4,}'
)

def has_count_sequence(text: str) -> bool:
    """Return True if text contains a block of numeric / '-' tokens (count row)."""
    if not isinstance(text, str):
        return False
    return bool(count_pattern.search(text))

# Find all rows with the phrase "Final in-season estimate"
final_mask = df["text_line"].astype(str).str.lower().str.contains(phrase)
final_indices = sorted(df.index[final_mask])

print(f"🔎 Found {len(final_indices)} 'Final in-season estimate' rows")

rows_to_drop = set()
count_rows_removed_indices = set()

# ------------------------------------------------------------
# For each "Final in-season estimate" line, drop it and its count row
# ------------------------------------------------------------
for idx in final_indices:
    # Always drop the "Final in-season estimate" line itself
    rows_to_drop.add(idx)
    final_text = df.at[idx, "text_line"]

    # Case 1: The same line also contains the count sequence
    if has_count_sequence(final_text):
        count_rows_removed_indices.add(idx)
        continue

    # Case 2: Look upwards for a count row on the same pdf_name/page_num
    pdf_name = df.at[idx, "pdf_name"] if "pdf_name" in df.columns else None
    page_num = df.at[idx, "page_num"] if "page_num" in df.columns else None

    for offset in range(1, 6):  # look up to five rows back
        candidate = idx - offset
        if candidate < df.index.min():
            break

        # If we have pdf_name/page_num, require they match (same report & page)
        if pdf_name is not None and "pdf_name" in df.columns:
            if df.at[candidate, "pdf_name"] != pdf_name:
                continue
        if page_num is not None and "page_num" in df.columns:
            if df.at[candidate, "page_num"] != page_num:
                continue

        candidate_text = df.at[candidate, "text_line"]
        if has_count_sequence(candidate_text):
            rows_to_drop.add(candidate)
            count_rows_removed_indices.add(candidate)
            break  # stop after first count row found

# ------------------------------------------------------------
# Apply removals
# ------------------------------------------------------------
df_cleaned = df.drop(index=rows_to_drop)

before = len(df)
after  = len(df_cleaned)
removed = before - after
count_rows_removed = len(count_rows_removed_indices)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df_cleaned.to_csv(output_clean, index=False)
df_cleaned.to_csv(output_recent, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
print(f"🗑️ Removed {removed:,} rows in total")
print(f"   • 'Final in-season estimate' rows removed: {len(final_indices)}")
if count_rows_removed:
    print(f"   • Associated count rows removed: {count_rows_removed}")
else:
    print("   • No associated count rows detected by pattern")
print(f"📊 Remaining rows: {after:,}")
print(f"💾 Saved cleaned copy → {output_clean}")
print(f"🔄 Updated csv_recent.csv with cleaned data")
print("✅ Step 1 complete.")