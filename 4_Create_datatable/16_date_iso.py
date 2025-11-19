# 16_date_iso.py
# ------------------------------------------------------------
# Normalize the "date" column (MM/DD/YY or MM/DD/YYYY)
# into ISO format (YYYY-MM-DD).
# Only modifies rows with valid date strings.
# Input  : 100_Data/csv_recent.csv
# Output : 100_Data/16_date_iso_output.csv + updated csv_recent.csv
# ------------------------------------------------------------

import pandas as pd
from datetime import datetime
from pathlib import Path
import os

# ------------------------------------------------------------
# Setup paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "16_date_iso_output.csv"
recent_path = data_dir / "csv_recent.csv"

print("🏗️  Step 16: Converting date → date_iso...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun Step 15 first.")

df = pd.read_csv(input_path)

if "date" not in df.columns:
    raise ValueError("❌ 'date' column not found in input file.")

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
# Save outputs (in 100_Data)
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
filled = df["date_iso"].astype(str).str.strip().ne("").sum()
total = len(df)

print(f"✅ date_iso conversion complete → {output_path}")
print(f"🔄 csv_recent.csv updated with date_iso column")
print(f"📊 {filled} of {total} rows successfully converted to ISO format.")