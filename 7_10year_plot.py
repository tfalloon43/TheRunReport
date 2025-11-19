# 7_10year_plot.py
# ------------------------------------------------------------
# Master controller for modular 10-year plotting pipeline.
# Executes all scripts in /7_10year_plot/ sequentially:
#   1_create_csv.py          → build csv_10av.csv from z3_data_clean
#   2_fishperday.py          → calculate fishperday metric
#   3_locationmarking.py     → append hatch/basin category columns
#   4_delete.py              → trim to last 10 yrs & allowed stocks
#   5_days.py                → explode date_iso into Day1–DayN columns
#   6_tablegen.py            → create empty H/W/U tables
#   7_tablefill.py           → populate tables with fishperday values
#   8_weeklydata.py          → aggregate to weekly 10-yr averages
#   50_check.py (optional)   → quick QC/visual checks
# Each sub-script outputs to 100_Data or 7_10year_plot as appropriate.
# ------------------------------------------------------------

import subprocess
import os

# ------------------------------------------------------------
# Base paths
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STEP_DIR = os.path.join(BASE_DIR, "7_10year_plot")

# ------------------------------------------------------------
# Ordered list of pipeline step scripts
# ------------------------------------------------------------
steps = [
    "1_create_csv.py",      # export z3_data_clean → csv_10av.csv
    "2_fishperday.py",      # compute fishperday metric
    "3_locationmarking.py", # add hatch/basin category columns
    "4_delete.py",          # filter rows (adult_diff_plot, stock, date range)
    "5_days.py",            # expand dates into Day1–DayN columns
    "6_tablegen.py",        # build empty stock tables for H/W/U
    "7_tablefill.py",       # fill tables with fishperday values
    "8_weeklydata.py",      # convert daily tables → weekly averages
    #"50_check.py",          # optional QA/visualization step
]

# ------------------------------------------------------------
# Execution
# ------------------------------------------------------------
print("🚀 Starting 10-Year Plot pipeline...\n")

# Ensure working directory is the step folder
os.chdir(STEP_DIR)
print(f"📂 Working directory set to: {STEP_DIR}\n")

# ------------------------------------------------------------
# Step-by-step execution loop
# ------------------------------------------------------------
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

    # --- Standard output ---
    if result.stdout.strip():
        print("🟢 Output:")
        print(result.stdout.strip())

    # --- Errors ---
    if result.stderr.strip():
        print("🔴 Errors:")
        print(result.stderr.strip())

    # --- Exit code handling ---
    if result.returncode != 0:
        print(f"❌ Step failed: {step} (exit code {result.returncode})")
        break
    else:
        print(f"✅ Completed: {step}\n")

print("\n✅ All 10-Year Plot steps completed (or stopped on error).")
