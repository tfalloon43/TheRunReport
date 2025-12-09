"""
step0_runner.py
-----------------------------------------
Coordinator script for EscapementReport_FishCounts.

Behavior:
    1) Run Step 1 to discover new PDF URLs.
    2) If no new PDFs are found, stop.
    3) If new PDFs are found, run Steps 2–30 in order.
"""

from pathlib import Path
import sys

# Ensure imports resolve when run from anywhere
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from step1_available_pdfs import main as step1_discover
import step2_download_pdfs
import step3_parse_pdfs
import step4_duplicate_db
import step5_pdf_name_rename
import step6_removeFISE
import step7_date_extract
import step8_stockpresence
import step9_stockpresence_lower
import step10_hatchery_name
import step11_textline2
import step12_textline3
import step13_count_data
import step14_textline4
import step15_textline5
import step16_textline6
import step17_StockBO
import step18_facility
import step19_species
import step20_family
import step21_date_iso
import step22_stock
import step23_counts
import step24_basin
import step25_dateblank_delete
import step26_hatcheryblank_delete
import step27_columnreorg
import step28_duplicates_delete
import step29_pdf_date
import step30_row_reorder


STEP_SEQUENCE = [
    ("Step 2: Download PDFs", step2_download_pdfs.main),
    ("Step 3: Parse PDFs", step3_parse_pdfs.main),
    ("Step 4: Duplicate DB table", step4_duplicate_db.main),
    ("Step 5: Rename PDF names in table", step5_pdf_name_rename.main),
    ("Step 6: Remove FISE rows", step6_removeFISE.main),
    ("Step 7: Extract date", step7_date_extract.main),
    ("Step 8: Stock presence", step8_stockpresence.main),
    ("Step 9: Stock presence lower", step9_stockpresence_lower.main),
    ("Step 10: Hatchery name", step10_hatchery_name.main),
    ("Step 11: Text line 2", step11_textline2.main),
    ("Step 12: Text line 3", step12_textline3.main),
    ("Step 13: Count data", step13_count_data.main),
    ("Step 14: Text line 4", step14_textline4.main),
    ("Step 15: Text line 5", step15_textline5.main),
    ("Step 16: Text line 6", step16_textline6.main),
    ("Step 17: Stock BO", step17_StockBO.main),
    ("Step 18: Facility", step18_facility.main),
    ("Step 19: Species", step19_species.main),
    ("Step 20: Family", step20_family.main),
    ("Step 21: Date ISO", step21_date_iso.main),
    ("Step 22: Stock", step22_stock.main),
    ("Step 23: Counts", step23_counts.main),
    ("Step 24: Basin", step24_basin.main),
    ("Step 25: Delete blank dates", step25_dateblank_delete.main),
    ("Step 26: Delete blank hatchery", step26_hatcheryblank_delete.main),
    ("Step 27: Column reorg", step27_columnreorg.main),
    ("Step 28: Delete duplicates", step28_duplicates_delete.main),
    ("Step 29: PDF date normalization", step29_pdf_date.main),
    ("Step 30: Row reorder", step30_row_reorder.main),
]


def run_pipeline():
    print("\n🚀 EscapementReport_FishCounts runner starting...\n")

    urls_to_process = step1_discover()  # Returns list of URLs with processed=0

    if not urls_to_process:
        print("✔ No new PDFs found — skipping Steps 2–30.\n")
        return

    print(f"🆕 Found {len(urls_to_process)} new PDF(s) — running Steps 2–30.\n")

    for label, func in STEP_SEQUENCE:
        print(f"▶ {label}")
        func()
        print()  # spacer between steps

    print("\n🏁 Escapement pipeline finished.\n")


if __name__ == "__main__":
    run_pipeline()
