"""
step2_download_pdfs.py
-----------------------------------------
Download PDFs that step1 flagged as unprocessed (processed = 0)
and store them temporarily.

Updates:
    • Writes SHA256 hash into EscapementReports.hash
    • Leaves processed = 0 (parsing happens in Step 3)
"""

import sqlite3
import hashlib
import requests
from pathlib import Path
import sys

# ------------------------------------------------------------
# Resolve backend paths
# ------------------------------------------------------------

# File lives in:
#   runreport-backend/EscapementReport_FishCounts/
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent          # runreport-backend/
DB_DIR = BACKEND_ROOT / "0_db"
DB_PATH = DB_DIR / "local.db"

# Temp folder NEXT TO this script:
TMP_DIR = CURRENT_DIR / "temp_pdfs"
TMP_DIR.mkdir(exist_ok=True)

print(f"🗄️ Using local DB at: {DB_PATH}")
print(f"📁 temp_pdfs folder: {TMP_DIR}")

# ------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_urls_to_download() -> list[str]:
    """Return report_url rows where processed = 0."""
    sql = "SELECT report_url FROM EscapementReports WHERE processed = 0;"
    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()
    return [row["report_url"] for row in rows]


def update_hash(url: str, hash_value: str):
    """Store the file hash in EscapementReports."""
    sql = """
        UPDATE EscapementReports
        SET hash = ?
        WHERE report_url = ?
    """
    with get_conn() as conn:
        conn.execute(sql, (hash_value, url))
        conn.commit()


# ------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def download_pdf(url: str) -> tuple[Path, bytes]:
    """Download a PDF into temp_pdfs and return (filepath, raw_bytes)."""

    filename = url.split("/")[-1]
    out_path = TMP_DIR / filename

    print(f"⬇️  Downloading: {filename}")

    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    out_path.write_bytes(resp.content)
    return out_path, resp.content


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    print("🔄 Step 2: Downloading unprocessed PDFs...")

    urls = get_urls_to_download()
    print(f"📥 PDFs to download: {len(urls)}")

    if not urls:
        print("✔ No PDFs need downloading. Step 2 complete.")
        return

    for url in urls:
        try:
            pdf_path, data = download_pdf(url)
            file_hash = sha256_bytes(data)

            print(f"   ✔ Saved: {pdf_path.name}")
            print(f"   🔑 Hash: {file_hash[:12]}...")

            update_hash(url, file_hash)

        except Exception as e:
            print(f"⚠️ ERROR downloading {url}: {e}")

    print("\n✅ Step 2 complete — PDFs downloaded & registry updated.")


if __name__ == "__main__":
    main()