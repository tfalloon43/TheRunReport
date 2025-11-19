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
STEP_DIR = os.path.join(BASE_DIR, "4_create_datatable")

# Ordered list of pipeline steps
steps = [
    "0_create_csv.py",
    "1_delete.py",
    "2_date_extract.py",
    "3_stock_presence.py",
    "4_stock_presence_lower.py",
    "5_hatchery_name.py",
    "6_TL2.py",
    "7_TL3.py",
    "8_count_data.py",
    "9_TL4.py",
    "10_TL5.py",
    "11_TL6.py",
    "12_Stock_BO.py",
    "13_facility.py",
    "14_species.py",
    "15_Family.py",
    "16_date_iso.py",
    "17_stock.py",
    "18_counts.py",
    "19_basin.py",
    #"20_stock_corrections.py", #if needed after plot data
    "21_export.py",
    "22_delete_csv.py",
    
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
