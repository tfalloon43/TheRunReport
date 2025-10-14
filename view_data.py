# view_data_v18_qa.py
# ------------------------------------------------------------
# Quality assurance viewer for create_datatable_v18 output
# 1. Finds rows with basin value but no count_data
#    → prints that row AND the one immediately below it.
# 2. Finds rows with basin value and count_data_revised starting with a letter.
# ------------------------------------------------------------

import pandas as pd
import re

# --- Load latest output file ---
input_file = "pdf_lines_labeled_full_v18.csv"
df = pd.read_csv(input_file)

# --- Normalize column names just in case ---
df.columns = [c.strip() for c in df.columns]

# --- Ensure required columns exist ---
required_cols = {"basin", "count_data", "count_data_revised"}
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"❌ Missing required columns in CSV: {missing}")

# ------------------------------------------------------------
# 1️⃣ Basin present but no count_data
# ------------------------------------------------------------
cond_no_count = (
    df["basin"].notna()
    & (df["basin"].astype(str).str.strip() != "")
    & (df["count_data"].isna() | (df["count_data"].astype(str).str.strip() == ""))
)

issue1_rows = df[cond_no_count].copy()
issue1_rows["issue_type"] = "No count_data"

# Add the row immediately below each issue for context
next_indices = issue1_rows.index + 1
next_indices = next_indices[next_indices < len(df)]
context1_rows = df.loc[next_indices].copy()
context1_rows["issue_type"] = "Context (row below)"

qa_issue1 = pd.concat([issue1_rows, context1_rows]).sort_index()

# ------------------------------------------------------------
# 2️⃣ Basin present and count_data_revised starts with a letter
# ------------------------------------------------------------
cond_letter_start = (
    df["basin"].notna()
    & (df["basin"].astype(str).str.strip() != "")
    & df["count_data_revised"].astype(str).str.match(r"^[A-Za-z]")
)

issue2_rows = df[cond_letter_start].copy()
issue2_rows["issue_type"] = "Count_data starts with letter"

# ------------------------------------------------------------
# Combine all issues
# ------------------------------------------------------------
qa_combined = pd.concat([qa_issue1, issue2_rows]).sort_index()

# Columns to show in the output
output_cols = [
    "pdf_name",
    "page_num",
    "text_line",
    "basin",
    "stock",
    "count_data",
    "count_data_revised",
    "issue_type",
]

# ------------------------------------------------------------
# Save and summarize
# ------------------------------------------------------------
output_file = "qa_view_v18_results.csv"
qa_combined[output_cols].to_csv(output_file, index=False)

print(f"✅ QA view saved → {output_file}")
print(f"🔹 Found {len(issue1_rows)} rows with no count_data")
print(f"🔹 Found {len(issue2_rows)} rows where count_data starts with a letter")
print(f"🔹 Total rows output: {len(qa_combined)}")