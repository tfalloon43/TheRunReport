"""
step0_runner.py
-----------------------------------------
Coordinator script for EscapementReport_FishCounts.

Behavior:
    1) Run Step 1 to discover new PDF URLs.
    2) If no new PDFs are found, stop.
    3) If new PDFs are found, run Steps 2–49 in order.
"""

from pathlib import Path
import sys
import runpy

# Ensure imports resolve when run from anywhere
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from step1_available_pdfs import main as step1_discover


STEP_FILES = [
    ("Step 2: Download PDFs", "step2_download_pdfs.py"),
    ("Step 3: Parse PDFs", "step3_parse_pdfs.py"),
    ("Step 4: Duplicate DB table", "step4_duplicate_db.py"),
    ("Step 5: Rename PDF names in table", "step5_pdf_name_rename.py"),
    ("Step 6: Remove FISE rows", "step6_removeFISE.py"),
    ("Step 7: Extract date", "step7_date_extract.py"),
    ("Step 8: Stock presence", "step8_stockpresence.py"),
    ("Step 9: Stock presence lower", "step9_stockpresence_lower.py"),
    ("Step 10: Hatchery name", "step10_hatchery_name.py"),
    ("Step 11: Text line 2", "step11_textline2.py"),
    ("Step 12: Text line 3", "step12_textline3.py"),
    ("Step 13: Count data", "step13_count_data.py"),
    ("Step 14: Text line 4", "step14_textline4.py"),
    ("Step 15: Text line 5", "step15_textline5.py"),
    ("Step 16: Text line 6", "step16_textline6.py"),
    ("Step 17: Stock BO", "step17_StockBO.py"),
    ("Step 18: Facility", "step18_facility.py"),
    ("Step 19: Species", "step19_species.py"),
    ("Step 20: Family", "step20_family.py"),
    ("Step 21: Date ISO", "step21_date_iso.py"),
    ("Step 22: Stock", "step22_stock.py"),
    ("Step 23: Counts", "step23_counts.py"),
    ("Step 24: Basin", "step24_basin.py"),
    ("Step 25: Delete blank dates", "step25_dateblank_delete.py"),
    ("Step 26: Delete blank hatchery", "step26_hatcheryblank_delete.py"),
    ("Step 27: Column reorg", "step27_columnreorg.py"),
    ("Step 28: Delete duplicates", "step28_duplicates_delete.py"),
    ("Step 29: PDF date normalization", "step29_pdf_date.py"),
    ("Step 30: Row reorder", "step30_row_reorder.py"),
    ("Step 31: Iteration 1", "step31_iteration1.py"),
    ("Step 32: Cleanup 1", "step32_cleanup1.py"),
    ("Step 33: Iteration 2", "step33_iteration2.py"),
    ("Step 34: Cleanup 2", "step34_cleanup2.py"),
    ("Step 35: Iteration 3", "step35_iteration3.py"),
    ("Step 36: Cleanup 3", "step36_cleanup3.py"),
    ("Step 37: Iteration 4", "step37_iteration4.py"),
    ("Step 38: Cleanup 4", "step38_cleanup4.py"),
    ("Step 39: Iteration 5", "step39_iteration5.py"),
    ("Step 40: Cleanup 5", "step40_cleanup5.py"),
    ("Step 41: Iteration 6", "step41_iteration6.py"),
    ("Step 42: Cleanup 6", "step42_cleanup6.py"),
    ("Step 43: Iteration 7", "step43_iteration7.py"),
    ("Step 44: Manual deletions", "step44_manualdeletions.py"),
    ("Step 45: Iteration 8", "step45_iteration8"),
    ("Step 46: Cleanup 8", "step46_cleanup8.py"),
    ("Step 47: Iteration 9", "step47_iteration9.py"),
    ("Step 48: Iteration 10", "step48_iteration10.py"),
    ("Step 49: Column reorg", "step49_column_reorg.py"),
]


def run_step(label: str, filename: str):
    path = CURRENT_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Step file missing: {path}")
    print(f"▶ {label}")
    runpy.run_path(str(path), run_name="__main__")
    print()


def run_pipeline():
    print("\n🚀 EscapementReport_FishCounts runner starting...\n")

    urls_to_process = step1_discover()  # Returns list of URLs with processed=0

    if not urls_to_process:
        print("✔ No new PDFs found — skipping Steps 2–49.\n")
        return

    print(f"🆕 Found {len(urls_to_process)} new PDF(s) — running Steps 2–49.\n")

    for label, filename in STEP_FILES:
        run_step(label, filename)

    print("\n🏁 Escapement pipeline finished.\n")


if __name__ == "__main__":
    run_pipeline()
