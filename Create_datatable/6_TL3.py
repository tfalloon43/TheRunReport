# 6_TL3.py
# ------------------------------------------------------------
# Build TL3 column from TL2
# TL3 = final 11 numeric/dash tokens at the end of TL2
# Conditions:
#   • Only active if TL2 is non-empty
#   • Matches numbers (with commas), dashes, or standalone digits
# Examples:
#   TL2 = "Green River- W 41 3 - - - 44 - - - - -" → TL3 = "41 3 - - - 44 - - - - -"
#   TL2 = "White River 3 - - - - - - 3 - - -"      → TL3 = "3 - - - - - - 3 - - -"
#   TL2 = "I-205- W 79 - 93,470 - - 79 - - - - -" → TL3 = "79 - 93,470 - - 79 - - - - -"
# Input  : csv_recent.csv
# Output : 6_TL3_output.csv + csv_recent.csv (updated snapshot)
# ------------------------------------------------------------

import os
import re
import pandas as pd

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "6_TL3_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 6: Creating TL3...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")

df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def extract_TL3(tl2):
    """Extract the final 11 tokens that are numbers, dashes, or comma-separated numbers."""
    if not isinstance(tl2, str) or not tl2.strip():
        return ""
    
    # Find all numeric/dash-like tokens
    tokens = re.findall(r"\d[\d,]*|-", tl2)
    if not tokens:
        return ""
    
    # Get the last 11 tokens
    selected = tokens[-11:]
    return " ".join(selected)

# ------------------------------------------------------------
# Apply logic
# ------------------------------------------------------------
df["TL3"] = df.apply(lambda r: extract_TL3(r["TL2"]) if isinstance(r.get("TL2"), str) and r["TL2"].strip() else "", axis=1)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
total_rows = len(df)
populated = df["TL3"].astype(str).str.strip().ne("").sum()
print(f"✅ TL3 extraction complete → {output_path}")
print(f"🔄 csv_recent.csv updated with TL3 column")
print(f"📊 {populated} of {total_rows} rows populated with TL3")