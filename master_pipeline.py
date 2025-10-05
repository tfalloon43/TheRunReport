# master_pipeline.py
import subprocess

# List of program files in execution order
pipeline_steps = [
   
   ## Populate clean data base ##
   ## From FishCounts folder ##

## Data Collection

    "FishCounts/download_pdfs.py",        # Step 1: download PDFs
    #"FishCounts/test_rename_pdfs.py",
    "FishCounts/rename_pdfs.py",          # Step 2: rename PDFs consistently

## Data Table generation/cleaning
    "FishCounts/MakeSQLiteTable.py",       # Step 3: create SQLite table from PDFs
    "FishCounts/test_create_datatable.py",
    "FishCounts/create_datatable.py",      # Step 4: structure data into escapement_data
    #"FishCounts/test_clean_tables.py",
    "FishCounts/Clean_tables.py",          # Step 5: clean data, remove unwanted rows
    
## Data management
    "FishCounts/test_prepare_plot_data.py",
    #"FishCounts/prepare_plot_data.py",     # Step 6: reorganize data table in escapement_data_cleaned


## Quality Checks
    #"FishCounts/view_data_specificpdf.py"  
    #"FishCounts/test.py",

## Not used
    #"FishCounts/plot_data_collection.py"    # Step 6: organizes datatable, calculates averages  

   ## 
]

for step in pipeline_steps:
    print(f"\n=== Running {step} ===")
    result = subprocess.run(["python3", step], capture_output=True, text=True)
    
    # Print stdout and stderr for each step
    print(result.stdout)
    if result.stderr:
        print(f"Errors/Warnings in {step}:\n{result.stderr}")