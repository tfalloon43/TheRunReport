# 12_flows.py
# ------------------------------------------------------------
# Master controller for the river flow pipeline.
#
# Executes all flow-processing scripts located in /12_flows/,
# allowing you to modularly:
#   • Pull USGS / NOAA gage flows
#   • Clean + normalize flow datasets
#   • Compute weekly/monthly aggregates
#   • Create unified flow outputs
#
# Each script inside /12_flows/ should output into 100_Data
# or inside 12_flows/ as appropriate.
# ------------------------------------------------------------

import subprocess
import os

# ------------------------------------------------------------
# Base paths
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STEP_DIR = os.path.join(BASE_DIR, "12_flows")

# ------------------------------------------------------------
# Ordered list of flow processing step scripts (USGS intake → NOAA merge)
# ------------------------------------------------------------
steps = [
    "1_collectrivers.py",     # Collect unique river names from hatchery/columbia tables into flows snapshots
    "2_USGSsites.py",         # Pull active WA USGS stations (8–10 digit site numbers) from NWIS
    "3_rivername.py",         # Extract canonical river_name from site_name and update flows.csv
    "4_merge1.py",            # Merge USGS site_name/site_number into flows.csv with clean Site/Gage numbering
    "5_flowpresence.py",      # Add flow_presence flag based on Site 1 and reorder columns
    "6_NOAAsites.py",         # Flag rivers still missing flow coverage for NOAA matching
    "7_NOAA_completelist.py", # Scrape full NOAA NWRFC gauge catalog to csv_NOAA_completelist.csv
    "8_delete_states.py",     # Filter NOAA catalog down to Washington-only stations
    "9_NOAA_SiteID.py",       # Derive NOAA Site ID from the ID column in the catalog
    "10_NOAAmerge.py",        # Merge NOAA station candidates into 6_NOAAsites.csv with Site/Gage columns
    "11_merge2.py",           # Append NOAA Site/Gage pairs into flows.csv without overwriting USGS data
]

# ------------------------------------------------------------
# Execution
# ------------------------------------------------------------
print("🌊🚀 Starting Flow Data Pipeline...\n")

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

print("\n🎉 All Flow Pipeline steps completed (or stopped on error).")
