# 8_TL4.py
# ------------------------------------------------------------
# Build TL4 column from TL2 and TL3
# TL4 = TL2 with TL3 (numeric tail) removed
# Example:
#   TL2 = "Big Soos Creek- H 8,796 207 - - - 2,431 - 279 3,413 29 2,851"
#   TL3 = "8,796 207 - - - 2,431 - 279 3,413 29 2,851"
#   TL4 = "Big Soos Creek- H"
# Conditions:
#   • Only active if TL2 and TL3 both exist and are non-empty
# Input  : csv_recent.csv
# Output : 8_TL4_output.csv + csv_recent.csv (updated snapshot)
# ------------------------------------------------------------

import os
import pandas as pd
import re

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "8_TL4_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 8: Creating TL4...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")
df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def make_TL4(tl2, tl3):
    """
    Remove the numeric tail (tl3) from tl2.
    - Must have both TL2 and TL3 values.
    - Result trimmed of trailing punctuation/spaces.
    """
    if not isinstance(tl2, str) or not tl2.strip():
        return ""
    if not isinstance(tl3, str) or not tl3.strip():
        return ""

    # Remove commas for consistency before comparison
    t2 = tl2.strip().replace(",", "")
    t3 = tl3.strip().replace(",", "")

    # TL3 might appear at the end; remove that segment
    if t2.endswith(t3):
        result = t2[: -len(t3)].strip()
    else:
        # fallback: try regex to remove numeric tail from end
        result = re.sub(r"(\s*\d[\d,\s-]*$)", "", t2).strip()

    # Clean any trailing hyphens or stray characters
    result = re.sub(r"[\s,-]+$", "", result)
    return result

# ------------------------------------------------------------
# Apply logic
# ------------------------------------------------------------
df["TL4"] = df.apply(
    lambda r: make_TL4(r.get("TL2", ""), r.get("TL3", "")), axis=1
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
populated = df["TL4"].astype(str).str.strip().ne("").sum()
print(f"✅ TL4 extraction complete → {output_path}")
print(f"🔄 csv_recent.csv updated with TL4 column")
print(f"📊 {populated} of {total_rows} rows populated with TL4")