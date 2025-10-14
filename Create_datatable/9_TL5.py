# 9_TL5.py
# ------------------------------------------------------------
# Build TL5 column:
#   • If stock_presence_lower has a value → TL5 = text_line
#   • Otherwise → TL5 = ""
#
# Input  : csv_recent.csv
# Output : 9_TL5_output.csv + csv_recent.csv (updated snapshot)
# ------------------------------------------------------------

import os
import pandas as pd

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "9_TL5_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 9: Creating TL5 (copy text_line if stock_presence_lower exists)...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")
df = pd.read_csv(input_path)

# Ensure required columns exist
required = ["text_line", "stock_presence_lower"]
for col in required:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# ------------------------------------------------------------
# Core logic
# ------------------------------------------------------------
df["TL5"] = df.apply(
    lambda r: str(r["text_line"]).strip()
    if isinstance(r["stock_presence_lower"], str) and r["stock_presence_lower"].strip()
    else "",
    axis=1,
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
populated = df["TL5"].astype(str).str.strip().ne("").sum()
print(f"✅ TL5 creation complete → {output_path}")
print(f"🔄 csv_recent.csv updated with TL5 column")
print(f"📊 {populated} of {total_rows} rows populated with TL5 (stock_presence_lower check)")