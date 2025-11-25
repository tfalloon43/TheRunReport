# create_datatable.py
# ------------------------------------------------------------
# Master controller for modular create_datatable pipeline.
# Runs sequentially inside /4_create_datatable/ and surfaces stdout/stderr.
# Flow (after PDF extraction):
#   0) Initialize CSV scaffold
#   1) Delete prior outputs
#   2) Extract dates
#   3-4) Normalize stock presence (upper/lower)
#   5) Normalize hatchery names
#   6-11) Apply TL2–TL6 transformations
#   12) Stock BO handling
#   13-15) Facility/species/family tagging
#   16) ISO dates
#   17-18) Stock + count calculations
#   19) Basin tagging
#   20) (optional) Stock corrections
#   21) Export table
#   22) Cleanup
# ------------------------------------------------------------

import subprocess
import os

# Base project directory (TheRunReport/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Folder containing all modular step scripts
STEP_DIR = os.path.join(BASE_DIR, "4_create_datatable")

# Ordered list of pipeline steps
steps = [
    "0_create_csv.py",        # Initialize CSV scaffold
    "1_delete.py",            # Clear prior outputs
    "2_date_extract.py",      # Extract dates
    "3_stock_presence.py",    # Stock presence (upper)
    "4_stock_presence_lower.py",  # Stock presence (lower)
    "5_hatchery_name.py",     # Normalize hatchery names
    "6_TL2.py",               # TL2 transform
    "7_TL3.py",               # TL3 transform
    "8_count_data.py",        # Count data prep
    "9_TL4.py",               # TL4 transform
    "10_TL5.py",              # TL5 transform
    "11_TL6.py",              # TL6 transform
    "12_Stock_BO.py",         # Stock BO handling
    "13_facility.py",         # Facility tagging
    "14_species.py",          # Species tagging
    "15_Family.py",           # Family tagging
    "16_date_iso.py",         # ISO date format
    "17_stock.py",            # Stock calculations
    "18_counts.py",           # Count calculations
    "19_basin.py",            # Basin tagging
    #"20_stock_corrections.py", # Optional: post-plot corrections
    "21_export.py",           # Export table
    "22_delete_csv.py",       # Cleanup
]

print("🚀 Starting create_datatable pipeline...\n")

# Change current working directory to STEP_DIR so all scripts output files here
os.chdir(STEP_DIR)
print(f"📂 Working directory set to: {STEP_DIR}\n")

for step in steps:
    step_path = os.path.join(STEP_DIR, step)
    if not os.path.exists(step_path):
        print(f"⚠️  Skipping missing file: {step}")
        continue

    print(f"▶ Running {step} ...")
    result = subprocess.run(
        ["python3", step_path],
        capture_output=True,
        text=True
    )

    # --- Print standard output ---
    if result.stdout.strip():
        print("🟢 Output:")
        print(result.stdout.strip())

    # --- Print any errors ---
    if result.stderr.strip():
        print("🔴 Errors:")
        print(result.stderr.strip())

    # --- Check success/failure ---
    if result.returncode != 0:
        print(f"❌ Step failed: {step} (exit code {result.returncode})")
        break
    else:
        print(f"✅ Completed: {step}\n")

print("\n✅ All steps completed (or stopped on error).")
