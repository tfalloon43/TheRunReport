# 5_data_clean.py
# ------------------------------------------------------------
# Master controller for modular data_clean pipeline.
# Runs inside /5_data_clean/ to tidy escapement_data:
#   1) Create initial CSV
#   2) Drop blank dates
#   3) Drop blank hatchery rows
#   4) Reorder/normalize columns
#   5) Remove duplicates
#   6) Export cleaned table
#   7) Cleanup
# Prints full stdout/stderr for visibility.
# ------------------------------------------------------------


import subprocess
import os

# Base project directory (TheRunReport/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Folder containing all modular step scripts
STEP_DIR = os.path.join(BASE_DIR, "5_data_clean")

# Ordered list of pipeline steps
steps = [
    "1_create_csv.py",       # Build initial cleaned CSV scaffold
    "2_date_blank.py",       # Drop rows with blank/invalid dates
    "3_hatchery_blank.py",   # Drop rows missing hatchery info
    "4_column_reorg.py",     # Reorder/normalize columns
    "5_remove_duplicates.py",# Remove duplicate rows
    "10_export.py",          # Export cleaned table
    "delete_csv.py",         # Cleanup temp CSVs
]

print("🚀 Starting clean_data pipeline...\n")

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
