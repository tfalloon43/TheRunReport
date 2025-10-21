# 2_pdf_date.py
# ------------------------------------------------------------
# Step 2: Extract PDF publication date from pdf_name column.
# Creates a new column 'pdf_date' in ISO format (YYYY-MM-DD).
# Example:
#   pdf_name = "WA_EscapementReport_01-02-2014.pdf"
#   → pdf_date = "2014-01-02"
# Input  : 100_Data/csv_plotdata.csv
# Output : 100_Data/2_pdf_date_output.csv + updated csv_plotdata.csv
# ------------------------------------------------------------

import pandas as pd
import re
from pathlib import Path

print("🏗️ Step 2: Extracting pdf_date (ISO) from pdf_name...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path = data_dir / "csv_plotdata.csv"
output_path = data_dir / "2_pdf_date_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}\nRun previous step first.")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# ------------------------------------------------------------
# Extract pdf_date from pdf_name
# ------------------------------------------------------------
def extract_pdf_date(pdf_name):
    if not isinstance(pdf_name, str):
        return ""
    match = re.search(r"(\d{2})-(\d{2})-(\d{4})", pdf_name)
    if not match:
        return ""
    mm, dd, yyyy = match.groups()
    try:
        return f"{yyyy}-{mm}-{dd}"
    except Exception:
        return ""

df["pdf_date"] = df["pdf_name"].apply(extract_pdf_date)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
nonblank = df["pdf_date"].astype(str).str.strip().ne("").sum()
print(f"✅ PDF date extraction complete → {output_path}")
print(f"🔄 csv_plotdata.csv updated with pdf_date column")
print(f"📊 {nonblank} of {len(df)} rows populated with valid pdf_date values.")
print("🎯 Example format: 2014-01-02")