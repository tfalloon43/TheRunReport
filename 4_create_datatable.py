# create_datatable.py
# ------------------------------------------------------------
# Master controller for modular create_datatable pipeline.
# Ensures each step runs inside /create_datatable/
# and prints full stdout/stderr for visibility.
# ------------------------------------------------------------

import subprocess
import os

# Base project directory (TheRunReport/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Folder containing all modular step scripts
STEP_DIR = os.path.join(BASE_DIR, "create_datatable")

# Ordered list of pipeline steps
steps = [
    "1_date_extract.py",
    "2_stock_presence.py",
    "3_stock_presence_lower.py",
    "4_hatchery_name.py",
    "5_TL2.py",
    "6_TL3.py",
    "7_count_data.py",
    "8_TL4.py",
    "9_TL5.py",
    "10_TL6.py",
    "11_Stock_BO.py",
    "12_Facility.py",
    "13_species.py",
    "14_Family.py",
    "15_date_iso.py",
    "16_stock.py",
    "17_counts.py",
    "100_view_data.py",
    
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