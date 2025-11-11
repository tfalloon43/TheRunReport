# 7_10year_plot.py
# ------------------------------------------------------------
# Master controller for modular 10-year plotting pipeline.
# Executes all step scripts in /7_10year_plot/ in sequential order.
# Each sub-script should output results to 100_Data or 7_10year_plot.
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
    "1_create_csv.py",
    "2_fishperday.py",
    "3_hatchspecies.py",
    "4_hatchfamily.py",
    "5_basinfamily.py",
    "5.1_basinspecies.py",
    "6_delete.py",
    "7_days.py",
    "8_tablegen.py",
    "9_tablefill.py",
    "10_weeklydata.py",
    #"",
    "50_check.py",
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