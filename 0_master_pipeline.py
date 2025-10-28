# master_pipeline.py — v2.0
# TheRunReport master automation pipeline
# -------------------------------------------------------
# This script runs the full data processing sequence from:
#   (1) Fetching PDFs from WDFW website
#   (2) Renaming & organizing files
#   (3) Extracting text → SQLite database
#   (4) Creating structured escapement tables
#   (5) Cleaning / filtering data
#   (6) Preparing final ordered datasets for plotting
# -------------------------------------------------------

import subprocess

# List of program files in the correct execution order
pipeline_steps = [


    # =====================================================
    # 1️⃣  RAW DATA COLLECTION — from WDFW website
    # =====================================================
    "100_Data/lookup_maps.py", #Step 0: Make sure directories are up to date
    "1_download_pdfs.py",      # Step 1: Download latest escapement PDFs to user's Desktop folder
    "2_rename_pdfs.py",        # Step 2: Standardize file names (e.g., WA_EscapementReport_09-21-2023.pdf)

    # =====================================================
    # 2️⃣  DATABASE CREATION — populate pdf_data.sqlite
    # =====================================================
    "3_MakeSQLiteTable.py",    # Step 3: Extract every line of text from PDFs into pdf_data.sqlite (table: pdf_lines)

    # =====================================================
    # 3️⃣  ESCAPEMENT DATA TABLES — build & clean datasets
    # =====================================================
    "4_create_datatable.py",   # Step 4: Convert raw pdf_lines → structured escapement_data table
    "5_data_clean.py",       # Step 5: Remove headers, duplicates, blank rows, invalid dates, etc.

    # =====================================================
    # 4️⃣  DATA PREPARATION — ready for analysis / plotting
    # =====================================================
    "6_prepare_plot_data.py",  # Step 6: Sort escapement_data_cleaned, compute days_since_last, and save escapement_reordered/reduced

    # =====================================================
    # 5️⃣  QUALITY CHECKS (optional manual inspection)
    # =====================================================
    # "view_data.py",        # Optional: Export SQLite tables to CSV for inspection
    # "test.py",             # Optional: Developer testing file

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