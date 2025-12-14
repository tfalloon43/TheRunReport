# 6_prepare_plot_data.py
# ------------------------------------------------------------
# Master controller for modular prepare_plot_data pipeline.
# Runs inside /6_prepare_plot_data/ to generate plot-ready tables:
#   1) Create base CSV and ingest pdf_data
#   2-4) Reorder and day-diff calculations
#   5-16) Adult/surplus/by-length/by-short transforms + de-dupes
#   17-32) Iterative condense/cleanup passes and final prep
#   50) Export
#   cleanup) Remove temp CSVs
# Prints full stdout/stderr for visibility.
# ------------------------------------------------------------

import subprocess
import os

# Base project directory (TheRunReport/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Folder containing all modular step scripts
STEP_DIR = os.path.join(BASE_DIR, "6_prepare_plot_data")

# Ordered list of pipeline steps
steps = [
    "1_csv_create.py",            # Base CSV scaffold
    "2_pdf_data.py",              # Ingest pdf_data
    "3_reorder.py",               # Initial reorder
    "3.5_date_AT_same_remove.py", # Drop same date_iso+Adult_Total keeping earliest pdf_date
    "4_day_diff.py",              # Day difference calc
    "5_adult_diff.py",            # Adult difference calc
    "6_surplus_diff.py",          # Surplus difference calc
    "7_by_adult.py",              # Adult grouping
    "8_by_adult_length.py",       # Adult by length
    "9_by_short.py",              # Short grouping
    "10_Xcount.py",               # Xcount calc
    "11_x_remove_dupes.py",       # Remove dupes
    "12_diff2.py",                # Diff pass 2
    "13_by2.py",                  # By-group pass 2
    "14_by_adult_length2.py",     # Adult length pass 2
    "15_by_short2.py",            # Short pass 2
    "16_xcount2.py",              # Xcount pass 2
    "17_x2_condense.py",          # Condense pass 2
    "18_iteration3.py",           # Iteration 3
    "19_x3_condense.py",          # Condense pass 3
    "20_iteration4.py",           # Iteration 4
    "21_x4_condense.py",          # Condense pass 4
    "22_iteration5.py",           # Iteration 5
    "23_reorder5.py",             # Reorder pass 5
    "24_iteration6.py",           # Iteration 6
    "25_cleanup6.py",             # Cleanup 6
    "26_iteration7.py",           # Iteration 7
    "27_cleanup7.py",             # Cleanup 7
    "28_iteration8.py",           # Iteration 8
    "29_cleanup9.py",             # Cleanup 9
    "30_iteration9.py",           # Iteration 9
    "31_iteration10.py",          # Iteration 10
    "32_prepare_plot_data_final.py",  # Final prep
    "50_export.py",               # Export plot data
    "delete_csv.py"               # Cleanup temp CSVs
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
