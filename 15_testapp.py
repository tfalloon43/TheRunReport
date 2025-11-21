# 15_testapp.py
# ------------------------------------------------------------
# Master controller for the modular Test App pipeline.
# Executes step scripts in /15_testapp/ sequentially.
#
# Purpose:
#   This pipeline will drive whatever processing/aggregation/
#   export logic you want for the test version of your app.
#
# Each sub-script should output results into either:
#   • 100_Data/
#   • 15_testapp/
#
# ------------------------------------------------------------

import subprocess
import os

# ------------------------------------------------------------
# Base paths
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STEP_DIR = os.path.join(BASE_DIR, "15_testapp")

# ------------------------------------------------------------
# Ordered list of pipeline step scripts
# (You will define what each step does next.)
# ------------------------------------------------------------
steps = [
    #"1_starter.py",      # (placeholder)
    "2_testapp.py",        # (placeholder)
    #"3_generate_app_tables.py",       # (placeholder)
    #"4_export_app_ready_data.py",     # (placeholder)
    # Add more steps as needed...
]

# ------------------------------------------------------------
# Execution
# ------------------------------------------------------------
print("🚀 Starting Test App pipeline...\n")

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

print("\n✅ All Test App steps completed (or stopped on error).")
