import sqlite3
import pdfplumber
from pathlib import Path
import os

# --- Paths ---
# Dynamically find the user's Desktop folder
desktop_path = Path(os.path.expanduser("~")) / "Desktop"

# Folder where PDFs are downloaded
pdf_folder = desktop_path / "RunReport_PDFs"

# SQLite database stored in the same folder as this script (so it's visible in VS Code)
db_path = Path(__file__).parent / "pdf_data.sqlite"

# --- Connect to the database ---
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# --- Step 1: Create table if it doesn't exist ---
cursor.execute("""
CREATE TABLE IF NOT EXISTS pdf_lines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pdf_name TEXT,
    page_num INTEGER,
    text_line TEXT
)
""")

conn.commit()

# --- Step 2: Loop through all PDFs ---
if not pdf_folder.exists():
    raise FileNotFoundError(f"❌ Folder not found: {pdf_folder}")

pdf_files = sorted(pdf_folder.glob("*.pdf"))

if not pdf_files:
    print(f"⚠️ No PDFs found in {pdf_folder}")
else:
    for pdf_file in pdf_files:
        print(f"📄 Processing {pdf_file.name}...")
        try:
            with pdfplumber.open(pdf_file) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    text = page.extract_text()
                    if not text:
                        continue
                    for line in text.splitlines():
                        cursor.execute(
                            "INSERT INTO pdf_lines (pdf_name, page_num, text_line) VALUES (?, ?, ?)",
                            (pdf_file.name, page_num, line.strip())
                        )
        except Exception as e:
            print(f"⚠️ Error reading {pdf_file.name}: {e}")

conn.commit()
conn.close()
print(f"✅ All PDF lines inserted into SQLite database: {db_path.name}")
print(f"📂 You can open it in VS Code: {db_path.resolve()}")