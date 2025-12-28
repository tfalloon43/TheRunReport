"""
step3_parse_pdfs.py
-----------------------------------------
Reads each unprocessed PDF from temp_pdfs/,
extracts all text lines, appends them to Supabase,
and marks the PDF as processed in Supabase.

Tables:

EscapementReports
    id, report_url, report_year, processed, hash, processed_at

EscapementRawLines
    id, pdf_name, page_num, text_line
"""

from pathlib import Path
import pdfplumber
from datetime import datetime
import sys

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

# File location:
#   runreport-backend/EscapementReport_FishCounts/step3_parse_pdfs.py
CURRENT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = CURRENT_DIR.parent                   # runreport-backend/
sys.path.append(str(BACKEND_ROOT))

PDF_DIR = CURRENT_DIR / "temp_pdfs"                 # same folder Step 2 uses

print(f"📁 Reading PDFs from: {PDF_DIR}")

try:
    from publish.supabase_client import SupabaseConfigError, get_supabase_client
except Exception as e:
    raise ImportError(
        "❌ Could not import Supabase client helper. "
        f"Checked path: {BACKEND_ROOT / 'publish'}\n"
        f"Error: {e}"
    )


def get_supabase():
    try:
        return get_supabase_client()
    except SupabaseConfigError as exc:
        print(f"❌ Supabase not configured: {exc}")
        return None


def _fetch_rows(client, columns: str, filters: dict[str, object] | None = None) -> list[dict]:
    rows: list[dict] = []
    page_size = 1000
    start = 0

    while True:
        query = client.table("EscapementReports").select(columns).range(start, start + page_size - 1)
        if filters:
            for key, value in filters.items():
                query = query.eq(key, value)
        response = query.execute()
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase query failed: {response.error}")
        data = response.data or []
        rows.extend(data)
        if len(data) < page_size:
            break
        start += page_size

    return rows


def get_unprocessed_reports(client):
    """
    We only process PDFs that:
        - processed = 0
        - hash IS NOT NULL  (means Step 2 downloaded them)
    """
    rows = _fetch_rows(client, "id,report_url,hash", filters={"processed": 0})
    return [row for row in rows if row.get("hash")]


def mark_processed(client, report_id):
    response = (
        client.table("EscapementReports")
        .update({"processed": 1, "processed_at": datetime.utcnow().isoformat()})
        .eq("id", report_id)
        .execute()
    )
    if getattr(response, "error", None):
        raise RuntimeError(f"Supabase update failed: {response.error}")

def insert_lines_bulk(client, lines, chunk_size: int = 1000):
    """
    lines = list of (pdf_name, page_num, text_line)
    Append to EscapementRawLines in Supabase.
    """
    if not lines:
        return

    rows = [
        {"pdf_name": pdf_name, "page_num": page_num, "text_line": text_line}
        for pdf_name, page_num, text_line in lines
    ]

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        response = client.table("EscapementRawLines").insert(
            chunk,
            default_to_null=False,
        ).execute()
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase insert failed: {response.error}")


# ------------------------------------------------------------
# PDF parsing
# ------------------------------------------------------------

def parse_pdf(client, pdf_path: Path) -> int:
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
        insert_lines_bulk(client, batch)

    return total_lines


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    print("🔄 Step 3: Parsing downloaded PDFs...")

    client = get_supabase()
    if client is None:
        return

    reports = get_unprocessed_reports(client)
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
            count = parse_pdf(client, pdf_path)
            print(f"   ✔ Extracted {count} lines")

            # Mark DB entry as processed
            mark_processed(client, report_id)

            # Delete parsed PDF
            pdf_path.unlink(missing_ok=True)
            print(f"   🗑️ Deleted local copy: {filename}")

        except Exception as e:
            print(f"⚠️ ERROR parsing {filename}: {e}")

    print("\n✅ Step 3 complete — all available PDFs parsed and removed.")


if __name__ == "__main__":
    main()
