# 9_previousyear.py
# ------------------------------------------------------------
# Master controller for modular Previous-Year plotting pipeline.
# Executes all step scripts in /9_previousyear/ in sequential order.
#
# Each sub-script should output results to 100_Data or 9_previousyear/.
# ------------------------------------------------------------

import subprocess
import os

# ------------------------------------------------------------
# Base paths
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STEP_DIR = os.path.join(BASE_DIR, "9_previousyear")

# ------------------------------------------------------------
# Ordered list of pipeline step scripts
# ------------------------------------------------------------
# ⚠️ You will create each of these scripts inside 9_previousyear/
#     They should mirror the current-year versions but using
#     csv_previousyear.csv + *_previous naming.
steps = [
    "1_create_csv_previousyear.py",     # Export z4_plot_data → csv_previousyear.csv
    "2_fishperday_previous.py",         # Compute fish-per-day for previous-year data
    "3_locationmarking_previous.py",    # Add hatch/basin family/species combo columns
    "4_delete_previous.py",             # Filter out invalid rows (H/W only, etc.)
    "5_days_previous.py",               # Expand date_iso → Day1–DayN columns
    "6_tablegen_previous.py",           # Generate empty 366-day tables
    "7_tablefill_previous.py",          # Fill tables with previous-year data
    "8_weeklydata_previous.py",         # Aggregate daily → weekly totals
    #"9_unify_weekly_previous.py",       # (optional) unify previous-year files
    # "50_check_previous.py",          # optional visualization step
]

# ------------------------------------------------------------
# Execution
# ------------------------------------------------------------
print("🚀 Starting Previous-Year Plot pipeline...\n")

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

print("\n✅ All Previous-Year Plot steps completed (or stopped on error).")