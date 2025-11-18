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
    "6_surplus_diff.py",
    "7_by_adult.py",
    "8_by_adult_length.py",
    "9_by_short.py",
    "10_Xcount.py",
    "11_x_remove_dupes.py",
    "12_diff2.py",
    "13_by2.py",
    "14_by_adult_length2.py",
    "15_by_short2.py",
    "16_xcount2.py",
    "17_x2_condense.py",
    "18_iteration3.py",
    "19_x3_condense.py",
    "20_iteration4.py",
    "21_x4_condense.py",
    "22_iteration5.py",
    "23_reorder5.py",
    "24_iteration6.py",
    "25_cleanup6.py",
    "26_iteration7.py",
    "27_cleanup7.py",
    "28_iteration8.py",
    "29_cleanup9.py",
    "30_iteration9.py",
    "31_iteration10.py",
    "32_prepare_plot_data_final.py",
    "50_export.py",
    #"delete_csv.py"
 
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