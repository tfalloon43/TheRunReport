# 15_date_iso.py
# ------------------------------------------------------------
# Normalize the "date" column (MM/DD/YY or MM/DD/YYYY)
# into ISO format (YYYY-MM-DD).
# Only modifies rows with valid date strings.
# Input  : csv_recent.csv
# Output : 15_date_iso_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "15_date_iso_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 15: Converting date → date_iso...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")

df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def convert_to_iso(date_str):
    if not isinstance(date_str, str) or not date_str.strip():
        return ""
    date_str = date_str.strip()

    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(date_str, fmt)
            # Handle two-digit years before 1950 → assume 2000s
            if parsed.year < 1950:
                parsed = parsed.replace(year=parsed.year + 2000)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # No valid format found
    return ""

# ------------------------------------------------------------
# Apply conversion
# ------------------------------------------------------------
df["date_iso"] = df["date"].apply(convert_to_iso)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
filled = df["date_iso"].astype(str).str.strip().ne("").sum()
print(f"✅ date_iso conversion complete → {output_path}")
print(f"🔄 csv_recent.csv updated with date_iso column")
print(f"📊 {filled} rows successfully converted.")