# master_pipeline.py — v2.0
# TheRunReport master automation pipeline
# -------------------------------------------------------
# This script runs the full data processing sequence from:
#   (1) Fetching PDFs from WDFW website
#   (2) Renaming & organizing files
#   (3) Extracting text → SQLite database
#   (4) Creating structured escapement tables
#   (5) Cleaning / filtering data
#   (6) Preparing ordered datasets for plotting
#   (7) Building 10-year trend tables
#   (8) Computing current-year stats
#   (9) Computing previous-year stats
#   (10) Fetching Columbia Basin adult counts
#   (11) Unifying hatchery + Columbia outputs
#   (12) Optional QA app
# -------------------------------------------------------

import subprocess

# List of program files in the correct execution order
pipeline_steps = [


    # =====================================================
    # 1  RAW DATA COLLECTION — from WDFW website
    # =====================================================
    "lookup_maps.py",           # Step 0: Ensure lookup data/dirs exist
    "1_download_pdfs.py",       # Step 1: Download latest escapement PDFs to user's Desktop folder
    "2_Rename_pdfs.py",         # Step 2: Standardize file names (e.g., WA_EscapementReport_09-21-2023.pdf)

    # =====================================================
    # 2  DATABASE CREATION — populate pdf_data.sqlite
    # =====================================================
    "3_MakeSQLiteTable.py",     # Step 3: Extract every line of text from PDFs into pdf_data.sqlite (table: pdf_lines)

    # =====================================================
    # 3  ESCAPEMENT DATA TABLES — build & clean datasets
    # =====================================================
    "4_create_datatable.py",    # Step 4: Convert raw pdf_lines → structured escapement_data table
    "5_data_clean.py",          # Step 5: Remove headers, duplicates, blank rows, invalid dates, etc.

    # =====================================================
    # 4  FISH COUNT DATA PREPARATION — ready for analysis / plotting
    # =====================================================
    "6_prepare_plot_data.py",   # Step 6: Sort escapement_data_cleaned, compute days_since_last, and save escapement_reordered/reduced
    "7_10year_plot.py",         # Step 7: Compute 10-year averages for all basins/species
    "8_currentyear.py",         # Step 8: Compute current-year metrics for all basins/species
    "9_previousyear.py",        # Step 9: Extract previous full year metrics
    "10_columbiadata.py",       # Step 10: Fetch Columbia Basin daily adult counts from FPC
    "11_unify_fishcounts.py",   # Step 11: Merge hatchery + Columbia datasets

    # =====================================================
    # 5  QUALITY CHECKS (optional manual inspection)
    # =====================================================
    "15_testapp.py"            # Step 15: Test application for QA checks (manual step; file name kept for legacy)

]

# -------------------------------------------------------
# Run each step in sequence, printing results for visibility
# -------------------------------------------------------

for step in pipeline_steps:
    print(f"\n=== Running {step} ===")
    result = subprocess.run(["python3", step], capture_output=True, text=True)

    # Display normal output
    print(result.stdout)

    # Display any errors or warnings
    if result.stderr:
        print(f"⚠️  Errors/Warnings in {step}:\n{result.stderr}")

print("\n✅ Master pipeline completed successfully.")
