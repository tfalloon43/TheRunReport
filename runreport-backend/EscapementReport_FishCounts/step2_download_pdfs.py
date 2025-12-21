"""
step2_download_pdfs.py
-----------------------------------------
Download PDFs that step1 flagged as unprocessed (processed = 0)
and store them temporarily.

Updates:
    • Writes SHA256 hash into EscapementReports.hash (Supabase)
    • Leaves processed = 0 (parsing happens in Step 3)
"""

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
sys.path.append(str(BACKEND_ROOT))

# Temp folder NEXT TO this script:
TMP_DIR = CURRENT_DIR / "temp_pdfs"
TMP_DIR.mkdir(exist_ok=True)

print(f"📁 temp_pdfs folder: {TMP_DIR}")

# ------------------------------------------------------------
# Supabase helpers
# ------------------------------------------------------------

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


def get_urls_to_download(client) -> list[str]:
    """Return report_url rows where processed = 0."""
    rows = _fetch_rows(client, "report_url,processed", filters={"processed": 0})
    return [row["report_url"] for row in rows if row.get("report_url")]


def update_hash(client, url: str, hash_value: str) -> None:
    """Store the file hash in EscapementReports."""
    response = (
        client.table("EscapementReports")
        .update({"hash": hash_value})
        .eq("report_url", url)
        .execute()
    )
    if getattr(response, "error", None):
        raise RuntimeError(f"Supabase update failed: {response.error}")


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

    client = get_supabase()
    if client is None:
        return

    urls = get_urls_to_download(client)
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

            update_hash(client, url, file_hash)

        except Exception as e:
            print(f"⚠️ ERROR downloading {url}: {e}")

    print("\n✅ Step 2 complete — PDFs downloaded & registry updated.")


if __name__ == "__main__":
    main()
