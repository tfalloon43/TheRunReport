# MakeSQLiteTable.py
import sqlite3
import pdfplumber
from pathlib import Path

db_path = "pdf_data.sqlite"
pdf_folder = Path("pdf_reports")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Step 1: Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS pdf_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_num INTEGER,
    text_line TEXT
)
""")

# Step 2: Check if 'pdf_name' column exists
cursor.execute("PRAGMA table_info(pdf_lines)")
columns = [col[1] for col in cursor.fetchall()]
if "pdf_name" not in columns:
    cursor.execute("ALTER TABLE pdf_lines ADD COLUMN pdf_name TEXT")
    print("Added 'pdf_name' column to pdf_lines table.")

conn.commit()

# Step 3: Loop through PDFs
pdf_files = sorted(pdf_folder.glob("*.pdf"))
for pdf_file in pdf_files:
    print(f"Processing {pdf_file.name}...")
    with pdfplumber.open(pdf_file) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            lines = page.extract_text().splitlines()
            for line in lines:
                cursor.execute(
                    "INSERT INTO pdf_lines (page_num, pdf_name, text_line) VALUES (?, ?, ?)",
                    (page_num, pdf_file.name, line)
                )

conn.commit()
conn.close()
print("All PDF lines inserted into SQLite database.")
