"""
step3_parse_pdfs.py
-----------------------------------------
Reads each unprocessed PDF from temp_pdfs/,
extracts all text lines, stores them into local.db,
and marks the PDF as processed.

Tables:

EscapementReports
    id, report_url, report_year, processed, hash, processed_at

EscapementRawLines
    id, pdf_name, page_num, text_line
"""

import sqlite3
from pathlib import Path
import pdfplumber
from datetime import datetime

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

# File location:
#   runreport-backend/EscapementReport_FishCounts/step3_parse_pdfs.py
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent                   # runreport-backend/
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

PDF_DIR = CURRENT_DIR / "temp_pdfs"                 # same folder Step 2 uses

print(f"🗄️ Using DB: {DB_PATH}")
print(f"📁 Reading PDFs from: {PDF_DIR}")

# ------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_rawlines_table():
    sql = """
    CREATE TABLE IF NOT EXISTS EscapementRawLines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pdf_name TEXT,
        page_num INTEGER,
        text_line TEXT
    );
    """
    with get_conn() as conn:
        conn.execute(sql)
        conn.commit()


def get_unprocessed_reports():
    """
    We only process PDFs that:
        - processed = 0
        - hash IS NOT NULL  (means Step 2 downloaded them)
    """
    sql = """
    SELECT id, report_url
    FROM EscapementReports
    WHERE processed = 0 AND hash IS NOT NULL;
    """
    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()
    return rows


def mark_processed(report_id):
    sql = """
    UPDATE EscapementReports
    SET processed = 1,
        processed_at = ?
    WHERE id = ?
    """
    with get_conn() as conn:
        conn.execute(sql, (datetime.utcnow().isoformat(), report_id))
        conn.commit()


def insert_lines_bulk(lines):
    """
    lines = list of (pdf_name, page_num, text_line)
    Bulk insert inside ONE transaction for speed.
    """
    sql = """
    INSERT INTO EscapementRawLines (pdf_name, page_num, text_line)
    VALUES (?, ?, ?)
    """
    with get_conn() as conn:
        conn.executemany(sql, lines)
        conn.commit()


# ------------------------------------------------------------
# PDF parsing
# ------------------------------------------------------------

def parse_pdf(pdf_path: Path) -> int:
    """
    Extract all text lines from a PDF.
    Returns the number of text lines extracted.
    """

    total_lines = 0
    batch = []  # collect lines before bulk insertion

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()
            if not text:
                continue

            lines = text.splitlines()
            for line in lines:
                cleaned = line.strip()
                batch.append((pdf_path.name, page_num, cleaned))
            total_lines += len(lines)

    # Bulk insert
    if batch:
        insert_lines_bulk(batch)

    return total_lines


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    print("🔄 Step 3: Parsing downloaded PDFs...")

    ensure_rawlines_table()

    reports = get_unprocessed_reports()
    print(f"📄 PDFs needing parsing: {len(reports)}")

    if not reports:
        print("✔ No PDFs to process. Step 3 complete.")
        return

    for report in reports:
        report_id = report["id"]
        pdf_url = report["report_url"]
        filename = pdf_url.split("/")[-1]
        pdf_path = PDF_DIR / filename

        if not pdf_path.exists():
            print(f"⚠️ Missing PDF — expected: {filename}")
            continue

        print(f"📘 Parsing: {filename}")

        try:
            count = parse_pdf(pdf_path)
            print(f"   ✔ Extracted {count} lines")

            # Mark DB entry as processed
            mark_processed(report_id)

            # Delete parsed PDF
            pdf_path.unlink(missing_ok=True)
            print(f"   🗑️ Deleted local copy: {filename}")

        except Exception as e:
            print(f"⚠️ ERROR parsing {filename}: {e}")

    print("\n✅ Step 3 complete — all available PDFs parsed and removed.")


if __name__ == "__main__":
    main()