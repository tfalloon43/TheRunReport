# 11_unify_fishcounts.py
# ------------------------------------------------------------
# Master controller for unifying *all* fish-count outputs
# across:
#   • 7_10year_plot       → weekly_unified_long.csv
#   • 8_currentyear       → weekly_current_unified.csv
#   • 9_previousyear      → weekly_previous_unified.csv
#   • 10_columbiadata     → columbiadaily_long.csv
#
# Then merges everything into:
#       100_Data/fishcounts_unified.csv
#
# Each sub-script should output into either 100_Data or /11_unify_fishcounts/.
# ------------------------------------------------------------

import subprocess
import os

# ------------------------------------------------------------
# Base paths
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STEP_DIR = os.path.join(BASE_DIR, "11_unify_fishcounts")

# ------------------------------------------------------------
# Ordered list of pipeline step scripts
# ------------------------------------------------------------
steps = [
    "1_unify_weekly.py",         # merge weekly inputs (10yr/current/previous)
    "2_delete_csvs.py",          # clean out old CSVs
    "3_keep_basinfamily.py",     # keep only basinfamily category
    "4_river.py",                # extract river from identifier
    "5_Species_Plot.py",         # extract Species_Plot from identifier
    "6_stockmerge.py",           # add stock=ONE aggregates
    "7_deletestock.py",          # keep only stock=ONE
    "8_delete_columbia.py",      # drop Columbia River rows
    "8.1_delete_snake.py",       # drop Snake River rows
    "9_Snohomish.py",            # add Snohomish aggregated rows
    "10_reorg.py",               # reorder/trim columns
    "11_id.py",                  # add unique ID column
    "20_export.py",              # export to sqlite
    "21_delete.py",              # clean up intermediate files
]

# ------------------------------------------------------------
# Execution
# ------------------------------------------------------------
print("🚀 Starting Unified Fish Counts pipeline...\n")

# Ensure working directory is the step folder
if not os.path.exists(STEP_DIR):
    raise FileNotFoundError(f"❌ Missing folder: {STEP_DIR}")

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

    # Standard output
    if result.stdout.strip():
        print("🟢 Output:")
        print(result.stdout.strip())

    # Errors
    if result.stderr.strip():
        print("🔴 Errors:")
        print(result.stderr.strip())

    # Exit handling
    if result.returncode != 0:
        print(f"❌ Step failed: {step} (exit code {result.returncode})")
        break

    print(f"✅ Completed: {step}\n")

print("\n🎉 All Unified Fish Count steps completed (or stopped on error).")
