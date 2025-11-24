# 10_columbiadata.py
# ------------------------------------------------------------
# Master controller for the Columbia Daily Data pipeline.
#
# Executes all scripts inside /10_columbiadata/ in sequence.
# Each step should output results to 100_Data or 10_columbiadata/.
#
# Current steps:
#   1_datapull.py              → Pull 65 FPC daily count CSVs
#   (more steps will be added soon)
#
# Usage:
#   python3 10_columbiadata.py
# ------------------------------------------------------------

import subprocess
import os

# ------------------------------------------------------------
# Base paths
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STEP_DIR = os.path.join(BASE_DIR, "10_columbiadata")   # <-- CORRECT FOLDER NAME

# ------------------------------------------------------------
# Ordered list of pipeline step scripts
# ------------------------------------------------------------
steps = [
    "1_datapull.py",     # Pulls all Columbia/Snake dam daily data into unified CSV
    "2_species_plot.py", # create species_plot column
    "3_river.py",        # maps dam location to river
    "4_reorg.py",        # changes order of columns and gets rid of redundant
    # "99_check.py",     # (optional) visual QA
]

# ------------------------------------------------------------
# Execution
# ------------------------------------------------------------
print("🚀 Starting Columbia Daily Data Pipeline...\n")

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

print("\n✅ All Columbia Daily Data steps completed (or stopped on error).")