"""
step1_available_pdfs.py
-----------------------------------------
Discover escapement PDF URLs and store them in local.db
(simulating a Supabase registry).

Table: EscapementReports
"""

from pathlib import Path
import requests
from bs4 import BeautifulSoup
import sqlite3
import sys

# ------------------------------------------------------------
# Load SQLiteManager from runreport-backend/0_db
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent

# step1_available_pdfs.py lives in:
#   runreport-backend/EscapementReport_FishCounts/
# so backend root is ONE level up (parents[0])
BACKEND_ROOT = CURRENT_DIR.parent
DB_DIR = BACKEND_ROOT / "0_db"

# Add db folder to import path
import sys
sys.path.append(str(DB_DIR))

try:
    from sqlite_manager import SQLiteManager
except Exception as e:
    raise ImportError(
        f"❌ Could not import SQLiteManager.\n"
        f"Checked path: {DB_DIR}\n"
        f"Error: {e}"
    )


# ------------------------------------------------------------
# DB helpers (now using SQLiteManager)
# ------------------------------------------------------------

def ensure_table(db: SQLiteManager):
    sql = """
    CREATE TABLE IF NOT EXISTS EscapementReports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_url   TEXT NOT NULL UNIQUE,
        report_year  INTEGER,
        processed    INTEGER DEFAULT 0,
        hash         TEXT,
        processed_at TEXT
    );
    """
    db.conn.execute(sql)
    db.conn.commit()


def get_existing_urls(db: SQLiteManager) -> set[str]:
    rows = db.conn.execute("SELECT report_url FROM EscapementReports").fetchall()
    return {row[0] for row in rows}


def insert_new_urls(db: SQLiteManager, urls: list[str]) -> list[str]:
    existing = get_existing_urls(db)
    new_urls = []

    for url in urls:
        if url in existing:
            continue

        # Try to extract year from filename
        year = None
        fname = url.split("/")[-1]
        tokens = fname.replace(".pdf", "").replace("-", "_").split("_")
        for token in tokens:
            if token.isdigit() and len(token) == 4:
                year = int(token)
                break

        try:
            db.conn.execute(
                """
                INSERT INTO EscapementReports (report_url, report_year, processed)
                VALUES (?, ?, 0)
                """,
                (url, year),
            )
            new_urls.append(url)

        except sqlite3.IntegrityError:
            # Already exists
            pass

    db.conn.commit()
    return new_urls


def urls_needing_download(db: SQLiteManager) -> list[str]:
    rows = db.conn.execute(
        "SELECT report_url FROM EscapementReports WHERE processed = 0"
    ).fetchall()
    return [row[0] for row in rows]


# ------------------------------------------------------------
# Scrape WDFW page for PDF URLs
# ------------------------------------------------------------

BASE_URL = "https://wdfw.wa.gov/fishing/management/hatcheries/escapement"

def discover_pdf_urls() -> list[str]:
    print(f"🌐 Fetching: {BASE_URL}")
    resp = requests.get(BASE_URL, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    pdfs = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.lower().endswith(".pdf"):
            if href.startswith("http"):
                pdfs.append(href)
            else:
                pdfs.append(requests.compat.urljoin(BASE_URL, href))

    return pdfs


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main() -> list[str]:
    print("🔍 Step 1: Discovering escapement PDF URLs...")

    # Unified DB
    db = SQLiteManager()

    ensure_table(db)

    all_urls = discover_pdf_urls()
    print(f"📄 Found {len(all_urls)} PDF links on WDFW page")

    new_urls = insert_new_urls(db, all_urls)
    print(f"🆕 Inserted {len(new_urls)} new URLs into EscapementReports")

    todo = urls_needing_download(db)
    print(f"⬇️ PDFs needing download (processed = 0): {len(todo)}")

    return todo


if __name__ == "__main__":
    urls = main()