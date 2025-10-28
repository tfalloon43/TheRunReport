# 6_prepare_plot_data.py
# ------------------------------------------------------------
# Master controller for modular prepare_plot_data pipeline.
# Ensures each step runs inside /6_prepare_plot_data/
# and prints full stdout/stderr for visibility.
# ------------------------------------------------------------

import subprocess
import os

# Base project directory (TheRunReport/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Folder containing all modular step scripts
STEP_DIR = os.path.join(BASE_DIR, "6_prepare_plot_data")

# Ordered list of pipeline steps
steps = [
    "1_csv_create.py",
    "2_pdf_data.py",
    "3_reorder.py",
    "4_day_diff.py",
    "5_adult_diff.py",
    "6_bio_year.py",
    "7_remove_singles.py",
    "8_bio_length.py",
    "",
    "",
    "",
 
    # Add additional steps here as you build them
]

print("🚀 Starting prepare_plot_data pipeline...\n")

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