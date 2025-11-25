# 8_currentyear.py
# ------------------------------------------------------------
# Master controller for modular Current-Year plotting pipeline.
# Executes all step scripts in /8_currentyear/ in sequential order.
# Each sub-script should output results to 100_Data or 8_currentyear/.
# ------------------------------------------------------------

import subprocess
import os

# ------------------------------------------------------------
# Base paths
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STEP_DIR = os.path.join(BASE_DIR, "8_currentyear")

# ------------------------------------------------------------
# Ordered list of pipeline step scripts
# ------------------------------------------------------------
steps = [
    "1_create_csv_currentyear.py",          # Export z4_plot_data → csv_currentyear.csv
    "2_fishperday_current.py",              # Compute fish-per-day for current-year data
    "3_locationmarking_current.py",         # Add hatch/basin family/species combo columns
    "4_delete_current.py",                  # Filter out invalid rows (H/W only, etc.)
    "5_days_current.py",                    # Expand date_iso → Day1–DayN columns
    "6_tablegen_current.py",                # Generate empty 366-day tables
    "7_tablefill_current.py",               # Fill tables with current-year data
    "8_weeklydata_current.py",              # Aggregate daily → weekly totals
]

# ------------------------------------------------------------
# Execution
# ------------------------------------------------------------
print("🚀 Starting Current-Year Plot pipeline...\n")

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

print("\n✅ All Current-Year Plot steps completed (or stopped on error).")