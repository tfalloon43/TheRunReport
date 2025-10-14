# 5_TL2.py
# ------------------------------------------------------------
# Create a new column TL2:
#   • Only active (non-blank) if a date value exists.
#   • TL2 = text_line with Hatchery_Name and date (and everything after date) removed.
#   • Everything before Hatchery_Name remains intact.
# Input  : csv_recent.csv  (from previous step)
# Output : 5_tl2_output.csv + csv_recent.csv (updated snapshot)
# ------------------------------------------------------------

import os
import re
import pandas as pd

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "5_tl2_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 5: Building TL2 column...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")

df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def build_tl2(row):
    text = str(row.get("text_line", "")).strip()
    hatchery = str(row.get("Hatchery_Name", "")).strip()
    date = str(row.get("date", "")).strip()

    # TL2 only active if a date exists
    if not date or date == "nan" or not isinstance(date, str):
        return ""

    # Step 1 — remove the hatchery name (only if it exists and is not blank)
    if hatchery:
        pattern = re.escape(hatchery)
        text = re.sub(rf"^{pattern}\s*", "", text).strip()

    # Step 2 — remove the date and everything after it
    text = re.sub(rf"\s*{re.escape(date)}.*$", "", text).strip()

    return text

# ------------------------------------------------------------
# Apply logic
# ------------------------------------------------------------
df["TL2"] = df.apply(build_tl2, axis=1)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

print(f"✅ TL2 column created → {output_path}")
print(f"🔄 csv_recent.csv updated with TL2 column")