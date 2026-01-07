"""
step1_available_pdfs.py
-----------------------------------------
Discover escapement PDF URLs and store them in Supabase.

Table: EscapementReports
"""

from pathlib import Path
import requests
from bs4 import BeautifulSoup

# ------------------------------------------------------------
# Resolve backend paths / imports
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent

BACKEND_ROOT = CURRENT_DIR.parent

import sys
sys.path.append(str(BACKEND_ROOT))

try:
    from publish.supabase_client import SupabaseConfigError, get_supabase_client
except Exception as e:
    raise ImportError(
        "❌ Could not import Supabase client helper. "
        f"Checked path: {BACKEND_ROOT / 'publish'}\n"
        f"Error: {e}"
    )


# ------------------------------------------------------------
# Supabase helpers
# ------------------------------------------------------------

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


def get_existing_urls(client) -> set[str]:
    rows = _fetch_rows(client, "report_url")
    return {row["report_url"] for row in rows if row.get("report_url")}


def insert_new_urls(client, urls: list[str]) -> list[str]:
    existing = get_existing_urls(client)
    seen_in_batch: set[str] = set()
    new_urls = []
    payload = []

    for url in urls:
        if url in existing or url in seen_in_batch:
            continue
        seen_in_batch.add(url)

        # Try to extract year from filename
        year = None
        fname = url.split("/")[-1]
        tokens = fname.replace(".pdf", "").replace("-", "_").split("_")
        for token in tokens:
            if token.isdigit() and len(token) == 4:
                year = int(token)
                break

        payload.append(
            {
                "report_url": url,
                "report_year": year,
                "processed": 0,
            }
        )
        new_urls.append(url)

    if payload:
        # EscapementReports has a UNIQUE constraint on report_url, so on_conflict
        # must target report_url to skip existing PDFs without overwriting rows.
        response = (
            client.table("EscapementReports")
            .upsert(
                payload,
                on_conflict="report_url",
                default_to_null=False,
            )
            .execute()
        )
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase insert failed: {response.error}")

    return new_urls


def urls_needing_download(client) -> list[str]:
    rows = _fetch_rows(client, "report_url,processed", filters={"processed": 0})
    return [row["report_url"] for row in rows if row.get("report_url")]


# ------------------------------------------------------------
# Scrape WDFW page for PDF URLs
# ------------------------------------------------------------

BASE_URL = "https://wdfw.wa.gov/fishing/management/hatcheries/escapement"

def discover_pdf_urls() -> list[str]:
    """
    Scrape the page and collect any link that points to a PDF.

    Robustness tweaks:
        • Strip query/fragment before checking for ".pdf"
        • Capture any href that CONTAINS ".pdf" (case-insensitive)
    """
    print(f"🌐 Fetching: {BASE_URL}")
    resp = requests.get(BASE_URL, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    pdfs = []
    skipped = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        # Strip query/fragment for extension detection
        clean_href = href.split("?", 1)[0].split("#", 1)[0]
        lower_href = clean_href.lower()

        if ".pdf" in lower_href:
            if href.startswith("http"):
                pdfs.append(href)
            else:
                pdfs.append(requests.compat.urljoin(BASE_URL, href))
        else:
            # Keep a short list of skipped hrefs for debugging
            if ".pdf" in href.lower():
                skipped.append(href)

    if skipped:
        print(f"⚠️  Skipped {len(skipped)} hrefs containing '.pdf' after query/fragment stripping (likely unexpected format):")
        for h in skipped[:5]:
            print(f"   • {h}")
        if len(skipped) > 5:
            print("   • …")

    return pdfs


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main() -> list[str]:
    print("🔍 Step 1: Discovering escapement PDF URLs...")

    client = get_supabase()
    if client is None:
        return []

    all_urls = discover_pdf_urls()
    print(f"📄 Found {len(all_urls)} PDF links on WDFW page")

    new_urls = insert_new_urls(client, all_urls)
    print(f"🆕 Inserted {len(new_urls)} new URLs into EscapementReports")

    todo = urls_needing_download(client)
    print(f"⬇️ PDFs needing download (processed = 0): {len(todo)}")

    return todo


if __name__ == "__main__":
    urls = main()
