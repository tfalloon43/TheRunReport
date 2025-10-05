import os
import re
from datetime import datetime

# Directory where PDFs are stored
pdf_dir = "pdf_reports"   # <-- update if your PDFs are in another folder

# Regex patterns
pattern_mmddyy = re.compile(r"(\d{6})\.pdf$", re.IGNORECASE)  # e.g., 010214.pdf

# Allow an optional single letter right after the day, e.g., 11-03b-2022
# Using a non-capturing optional group (?:[A-Za-z])? so m,d,y stay as the first three groups.
pattern_mmddyyyy = re.compile(
    r"(\d{1,2})[-_](\d{1,2})(?:[A-Za-z])?[-_](\d{4})",
    re.IGNORECASE
)

for filename in os.listdir(pdf_dir):
    if not filename.lower().endswith(".pdf"):
        continue

    old_path = os.path.join(pdf_dir, filename)
    new_filename = None

    # Case 1: MMDDYY (e.g., 010214.pdf)
    match1 = pattern_mmddyy.search(filename)
    if match1:
        date_str = match1.group(1)  # "010214"
        try:
            dt = datetime.strptime(date_str, "%m%d%y")
            new_filename = f"WA_EscapementReport_{dt.strftime('%m-%d-%Y')}.pdf"
        except ValueError:
            print(f"Skipping (bad date): {filename}")
            continue

    # Case 2: MM[-_]DD(optional letter)[-_]YYYY inside a longer name
    else:
        match2 = pattern_mmddyyyy.search(filename)
        if match2:
            m, d, y = match2.groups()
            try:
                dt = datetime.strptime(f"{m}-{d}-{y}", "%m-%d-%Y")
                new_filename = f"WA_EscapementReport_{dt.strftime('%m-%d-%Y')}.pdf"
            except ValueError:
                print(f"Skipping (bad date): {filename}")
                continue

    # Rename if we found a valid new name
    if new_filename:
        new_path = os.path.join(pdf_dir, new_filename)
        if not os.path.exists(new_path):  # avoid overwrite
            os.rename(old_path, new_path)
            # print(f"Renamed: {filename} -> {new_filename}")
        else:
            print(f"Skipped (already exists): {new_filename}")
    else:
        print(f"No date pattern matched: {filename}")