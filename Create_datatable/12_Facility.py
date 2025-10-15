# 12_facility.py
# ------------------------------------------------------------
# Build facility column using hatch_name_map
# Source column: Hatchery_Name
# Only active for rows that have a date value
# If Hatchery_Name found in lookup_maps.hatch_name_map → standardized name
# Otherwise → keep the original Hatchery_Name
# If no date → facility = ""
# ------------------------------------------------------------

import os
import sys
import pandas as pd

# --- Fix for import path ---
# Add parent directory (TheRunReport) to sys.path so we can import lookup_maps
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lookup_maps import hatch_name_map  # ✅ import standardized hatchery name map

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "12_facility_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 12: Building facility column (from Hatchery_Name)...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")
df = pd.read_csv(input_path)

# Ensure required column exists
if "Hatchery_Name" not in df.columns:
    df["Hatchery_Name"] = ""

# ------------------------------------------------------------
# Apply mapping
# ------------------------------------------------------------
def map_facility(row):
    """Return standardized facility name if row has a date."""
    date_val = str(row.get("date", "")).strip()
    hatch_val = str(row.get("Hatchery_Name", "")).strip()

    if not date_val or date_val.lower() in ("nan", "none", ""):
        return ""

    if not hatch_val:
        return ""

    # Look up in hatch_name_map (case-insensitive)
    mapped = hatch_name_map.get(hatch_val.upper(), hatch_val.title())
    return mapped.strip()

df["facility"] = df.apply(map_facility, axis=1)

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
total_rows = len(df)
populated = df["facility"].astype(str).str.strip().ne("").sum()
print(f"✅ Facility mapping complete → {output_path}")
print(f"🔄 csv_recent.csv updated with facility column")
print(f"📊 {populated} of {total_rows} rows populated with facility names")