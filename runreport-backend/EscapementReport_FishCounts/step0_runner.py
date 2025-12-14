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
import argparse
import re

# ------------------------------------------------------------
# Quick debug controls
# ------------------------------------------------------------
# COPY/PASTE your target filenames here (use names from STEP_FILES below, e.g., "step4_duplicate_db.py").
# When set, the runner executes the contiguous block of steps between them (inclusive).
FIRST_STEP_NAME = "step4_duplicate_db.py"  # <-- paste START filename here, or leave None
LAST_STEP_NAME = "step37_iteration3.py"   # <-- paste END filename here, or leave None
# Toggle Step 1 discovery: set to False to skip the new-PDF check.
ENABLE_STEP1_DISCOVERY = False

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
    ("Step 28: PDF date normalization", "step28_pdf_date.py"),
    ("Step 29: Delete duplicates", "step29_duplicates_delete.py"),
    ("Step 30: Row reorder", "step30_row_reorder.py"),
    ("Step 31: Remove same date_iso/Adult_Total (earliest pdf_date)", "step31_date_AT_same_remove.py"),
    ("Step 32: Remove same date_iso keep largest Adult_Total", "step32_datesame_ATdiff_remove.py"),
    ("Step 33: Iteration 1", "step33_iteration1.py"),
    ("Step 34: Cleanup 1", "step34_cleanup1.py"),
    ("Step 35: Iteration 2", "step35_iteration2.py"),
    ("Step 36: Cleanup 2", "step36_cleanup2.py"),
    ("Step 37: Iteration 3", "step37_iteration3.py"),
    ("Step 38: Cleanup 3", "step38_cleanup3.py"),
    ("Step 39: Iteration 4", "step39_iteration4.py"),
    ("Step 40: Cleanup 4", "step40_cleanup4.py"),
    ("Step 41: Iteration 5", "step41_iteration5.py"),
    ("Step 42: Cleanup 5", "step42_cleanup5.py"),
    ("Step 43: Iteration 6", "step43_iteration6.py"),
    ("Step 44: Cleanup 6", "step44_cleanup6.py"),
    ("Step 45: Iteration 7", "step45_iteration7.py"),
    ("Step 46: Manual deletions", "step46_manualdeletions.py"),
    ("Step 47: Iteration 8", "step47_iteration8.py"),
    ("Step 48: Cleanup 8", "step48_cleanup8.py"),
    ("Step 49: Iteration 9", "step49_iteration9.py"),
    ("Step 50: Iteration 10", "step50_iteration10.py"),
    ("Step 51: Column reorg", "step51_column_reorg.py"),
]


def extract_step_number(label: str) -> int:
    match = re.search(r"Step\s+(\d+)", label)
    if not match:
        raise ValueError(f"Cannot parse step number from label: {label}")
    return int(match.group(1))


def resolve_step_name_to_number(name: str) -> int:
    """
    Given a filename (e.g., 'step4_duplicate_db.py'), return its step number.
    """
    for label, filename in STEP_FILES:
        if filename == name:
            return extract_step_number(label)
    raise ValueError(f"Step name not found: {name}")


def run_step(label: str, filename: str):
    path = CURRENT_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Step file missing: {path}")
    print(f"▶ {label}")
    runpy.run_path(str(path), run_name="__main__")
    print()


def filter_steps(start: int | None, end: int | None):
    """Return (num, label, filename) filtered to the requested range."""
    filtered = []
    for label, filename in STEP_FILES:
        num = extract_step_number(label)
        if start is not None and num < start:
            continue
        if end is not None and num > end:
            continue
        filtered.append((num, label, filename))
    return filtered


def run_pipeline(start: int | None = None, end: int | None = None, skip_discovery: bool = False, force_run: bool = False):
    print("\n🚀 EscapementReport_FishCounts runner starting...\n")

    min_step = min(extract_step_number(label) for label, _ in STEP_FILES)
    max_step = max(extract_step_number(label) for label, _ in STEP_FILES)

    # Override start/end with filename-based controls if provided.
    if FIRST_STEP_NAME:
        start = resolve_step_name_to_number(FIRST_STEP_NAME)
    if LAST_STEP_NAME:
        end = resolve_step_name_to_number(LAST_STEP_NAME)

    start = start or min_step
    end = end or max_step

    if start > end:
        raise ValueError(f"Start step ({start}) cannot be greater than end step ({end}).")

    if start < min_step or end > max_step:
        raise ValueError(f"Step range must be between {min_step} and {max_step}. Requested: {start}–{end}.")

    urls_to_process = None
    discovery_enabled = ENABLE_STEP1_DISCOVERY and not skip_discovery
    if discovery_enabled:
        urls_to_process = step1_discover()  # Returns list of URLs with processed=0

        if not urls_to_process and not force_run:
            print(f"✔ No new PDFs found — skipping Steps {start}–{end}.\n")
            return
    else:
        print("⏭️  Skipping Step 1 discovery per toggle/flags.")

    selected_steps = filter_steps(start, end)

    if urls_to_process is not None:
        print(f"🆕 Found {len(urls_to_process)} new PDF(s) — running Steps {start}–{end}.\n")
    else:
        print(f"🛠️  Debug run — running Steps {start}–{end} without discovery.\n")

    for _, label, filename in selected_steps:
        run_step(label, filename)

    print("\n🏁 Escapement pipeline finished.\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Run EscapementReport_FishCounts pipeline.")
    parser.add_argument("--start", type=int, help="First step number to run (default: earliest available).")
    parser.add_argument("--end", type=int, help="Last step number to run (default: latest available).")
    parser.add_argument("--skip-discovery", action="store_true", help="Skip Step 1 URL discovery.")
    parser.add_argument("--force-run", action="store_true", help="Run requested steps even if no new PDFs are found.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(start=args.start, end=args.end, skip_discovery=args.skip_discovery, force_run=args.force_run)
