# 7_count_data.py
# ------------------------------------------------------------
# Convert TL3 into normalized numeric format
# • Replace dashes ("-") with 0
# • Remove commas from numbers
# • Keep token order intact
# • Output joined numeric string (space-separated)
# Examples:
#   TL3 = "108 4 - - - - - 104 - 8 -"        → count_data = "108 4 0 0 0 0 0 104 0 8 0"
#   TL3 = "- - 1,740,000 - - - - - - - -"    → count_data = "0 0 1740000 0 0 0 0 0 0 0 0"
# Input  : csv_recent.csv
# Output : 7_count_data_output.csv + csv_recent.csv (updated snapshot)
# ------------------------------------------------------------

import os
import re
import pandas as pd

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "7_count_data_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 7: Creating count_data...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")

df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def normalize_count_data(tl3):
    """
    Turn TL3 into a numeric string:
    - Replace '-' with 0
    - Remove commas
    - Keep numbers separated by spaces
    """
    if not isinstance(tl3, str) or not tl3.strip():
        return ""

    # Split TL3 into tokens
    tokens = re.split(r"\s+", tl3.strip())

    normalized = []
    for t in tokens:
        if t == "-":
            normalized.append("0")
        else:
            cleaned = t.replace(",", "")
            # If it’s a number (int or float-like), keep it
            if re.match(r"^\d+$", cleaned):
                normalized.append(cleaned)
            else:
                # For safety, if something unexpected, replace with 0
                normalized.append("0")

    return " ".join(normalized)

# ------------------------------------------------------------
# Apply logic
# ------------------------------------------------------------
df["count_data"] = df.apply(lambda r: normalize_count_data(r["TL3"]) if isinstance(r.get("TL3"), str) and r["TL3"].strip() else "", axis=1)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
total_rows = len(df)
populated = df["count_data"].astype(str).str.strip().ne("").sum()
print(f"✅ count_data normalization complete → {output_path}")
print(f"🔄 csv_recent.csv updated with count_data column")
print(f"📊 {populated} of {total_rows} rows populated with count_data")