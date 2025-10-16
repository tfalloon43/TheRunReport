import os
import re
from datetime import datetime

# --- Locate the Desktop folder dynamically ---
desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")

# Folder where PDFs were downloaded
pdf_dir = os.path.join(desktop_path, "RunReport_PDFs")

# Ensure folder exists
if not os.path.exists(pdf_dir):
    raise FileNotFoundError(f"❌ Folder not found: {pdf_dir}")

print(f"📂 Renaming PDFs in: {pdf_dir}")

# --- Regex patterns ---
pattern_mmddyy = re.compile(r"(\d{6})\.pdf$", re.IGNORECASE)  # e.g., 010214.pdf

# Allow an optional single letter right after the day, e.g., 11-03b-2022
pattern_mmddyyyy = re.compile(
    r"(\d{1,2})[-_](\d{1,2})(?:[A-Za-z])?[-_](\d{4})",
    re.IGNORECASE
)

# --- Rename loop ---
for filename in os.listdir(pdf_dir):
    if not filename.lower().endswith(".pdf"):
        continue

    old_path = os.path.join(pdf_dir, filename)
    new_filename = None

    # Case 1: MMDDYY (e.g., 010214.pdf)
    match1 = pattern_mmddyy.search(filename)
    if match1:
        date_str = match1.group(1)
        try:
            dt = datetime.strptime(date_str, "%m%d%y")
            new_filename = f"WA_EscapementReport_{dt.strftime('%m-%d-%Y')}.pdf"
        except ValueError:
            print(f"⚠️  Skipping (bad date): {filename}")
            continue

    # Case 2: MM-DD-[optional letter]-YYYY
    else:
        match2 = pattern_mmddyyyy.search(filename)
        if match2:
            m, d, y = match2.groups()
            try:
                dt = datetime.strptime(f"{m}-{d}-{y}", "%m-%d-%Y")
                new_filename = f"WA_EscapementReport_{dt.strftime('%m-%d-%Y')}.pdf"
            except ValueError:
                print(f"⚠️  Skipping (bad date): {filename}")
                continue

    # --- Rename if valid new name found ---
    if new_filename:
        new_path = os.path.join(pdf_dir, new_filename)
        if not os.path.exists(new_path):  # Avoid overwriting
            os.rename(old_path, new_path)
            print(f"✅ Renamed: {filename} → {new_filename}")
        else:
            print(f"⏩ Skipped (already exists): {new_filename}")
    else:
        print(f"🚫 No date pattern matched: {filename}")

print("🎯 All possible files processed.")