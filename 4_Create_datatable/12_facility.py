# 12_facility.py
# ------------------------------------------------------------
# Build facility column using hatch_name_map
# Source column: Hatchery_Name
# Only active for rows that have a date value
# If Hatchery_Name found in lookup_maps.hatch_name_map → standardized name
# Otherwise → keep the original Hatchery_Name
# If no date → facility = ""
# ------------------------------------------------------------

import pandas as pd
import re
import sys
from pathlib import Path
import os


############
# ------------------------------------------------------------
# Setup imports and paths
# ------------------------------------------------------------
# Project structure:
# TheRunReport/
# ├── 4_Create_datatable/
# └── 100_data/lookup_maps.py
# ------------------------------------------------------------

project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_data"
data_dir.mkdir(exist_ok=True)

# 👇 Add 100_data folder to Python path so lookup_maps can be found
# --- Add TheRunReport/100_data to path dynamically ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
data_path = os.path.join(project_root, "100_data")
sys.path.append(data_path)

from lookup_maps import hatch_name_map # type: ignore

input_path = data_dir / "csv_recent.csv"
output_path = data_dir / "12_Facility_output.csv"
recent_path = data_dir / "csv_recent.csv"
#########

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