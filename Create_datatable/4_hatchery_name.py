# 4_hatchery_name.py
# ------------------------------------------------------------
# Extract Hatchery_Name values from text_line
# Conditions:
#   • Capture ≥2 consecutive ALL-CAPS words at the start of the line.
#   • Stop when a lowercase-starting word appears OR a stopword like "I-205" or "Stock-" appears.
#   • Fix cases like "HATCHERYPriest" by inserting a space.
#   • Ignore WDFW, CAUTION, or species headers (e.g., Fall Chinook).
#   • Apply manual corrections from lookup_maps.hatchery_name_corrections.
# Input  : csv_recent.csv
# Output : 4_hatchery_name_output.csv + csv_recent.csv
# ------------------------------------------------------------

import os
import re
import sys
import pandas as pd

# ------------------------------------------------------------
# Setup imports and paths
# ------------------------------------------------------------
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lookup_maps import hatchery_name_corrections

base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "4_hatchery_name_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 4: Extracting Hatchery_Name...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")
df = pd.read_csv(input_path)

# ------------------------------------------------------------
# Helper function
# ------------------------------------------------------------
def extract_hatchery_name(text):
    """
    Extracts consecutive ALL-CAPS words at the start of text_line until a lowercase word
    or a location-like token (I-, HWY, SR-, US-, STOCK-) appears.
    Fixes merged forms like 'HATCHERYPriest' by adding space.
    """
    if not isinstance(text, str) or not text.strip():
        return ""

    line = text.strip()

    # --- Fix glued patterns like HATCHERYPriest / HATCHERYStock / HATCHERYFinal ---
    line = re.sub(r"(HATCHERY)(?=[A-Z]?[a-z])", r"\1 ", line)
    line = re.sub(r"(HATCHERYP|HATCHERYF|HATCHERYR)(?=[A-Z]?[a-z])",
                  lambda m: m.group(1)[:-1] + " " + m.group(1)[-1], line)

    # --- Skip headers / species / warnings ---
    if re.match(r"^(WDFW|CAUTION)\b", line, re.IGNORECASE):
        return ""
    if re.match(r"^[A-Z][a-z]+\s+Chinook", line):
        return ""

    # --- Tokenize ---
    words = line.split()
    hatchery_parts = []

    # stop triggers like I-205, SR-, HWY, STOCK-
    stop_tokens = re.compile(r"^(I-|SR-|HWY|US-|STOCK-)", re.IGNORECASE)

    for w in words:
        # stop if lowercase-starting word or location/data prefix
        if re.match(r"^[A-Z][a-z]", w) or stop_tokens.match(w):
            break
        # only consider all-caps, but avoid punctuation noise
        if re.match(r"^[A-Z0-9&'()./-]+$", w):
            hatchery_parts.append(w)
        else:
            break

    if len(hatchery_parts) >= 2:
        return " ".join(hatchery_parts)
    return ""

# ------------------------------------------------------------
# Apply extraction and corrections
# ------------------------------------------------------------
df["Hatchery_Name"] = df["text_line"].apply(extract_hatchery_name)

def apply_hatchery_corrections(name):
    if not isinstance(name, str) or not name.strip():
        return name
    return hatchery_name_corrections.get(name.strip(), name.strip())

df["Hatchery_Name"] = df["Hatchery_Name"].apply(apply_hatchery_corrections)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

total_rows = len(df)
nonblank = df["Hatchery_Name"].astype(str).str.strip().ne("").sum()
print(f"✅ Hatchery_Name extraction complete → {output_path}")
print(f"🔄 csv_recent.csv updated with Hatchery_Name column")
print(f"📊 {nonblank} of {total_rows} rows populated with Hatchery_Name")
