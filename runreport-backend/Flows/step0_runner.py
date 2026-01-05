"""
step0_runner.py
-----------------------------------------
Coordinator script for the DB-backed Flows pipeline.

Runs Step 1 through Step 17 (or a selected range) by executing each
step script in order. Steps read/write tables in:
    runreport-backend/0_db/local.db

Usage:
    python3 step0_runner.py
    python3 step0_runner.py --start 7 --end 12
    python3 step0_runner.py --list
"""

from __future__ import annotations

import argparse
import os
import re
import runpy
import sys
import traceback
from pathlib import Path

# Ensure imports resolve when run from anywhere
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

STEP_FILES: list[tuple[str, str]] = [
    ("Step 1: Collect rivers", "step1_collectrivers.py"),
    ("Step 2: USGS sites", "step2_USGSsites.py"),
    ("Step 3: USGS river_name extraction", "step3_rivername.py"),
    ("Step 4: Merge USGS sites into Flows", "step4_merge1.py"),
    ("Step 5: flow_presence (USGS)", "step5_flowpresence.py"),
    ("Step 6: Identify NOAA candidate rivers", "step6_NOAAsites.py"),
    ("Step 7: NOAA complete list scrape", "step7_NOAA_completelist.py"),
    ("Step 8: Filter NOAA list to WA", "step8_delete_states.py"),
    ("Step 9: Derive NOAA Site ID", "step9_NOAA_SiteID.py"),
    ("Step 10: Merge NOAA stations into candidates", "step10_NOAAmerge.py"),
    ("Step 11: Append NOAA gauges into Flows", "step11_merge2.py"),
    ("Step 12: flow_presence (NOAA)", "step12_flowpresence2.py"),
    ("Step 13: Manual NOAA/custom gauges", "step13_manualNOAA.py"),
    ("Step 14: Cleanup (no-op)", "step14_delete.py"),
    ("Step 15: Fetch USGS flow/stage", "step15_USGSflow.py"),
    ("Step 16: Fetch NOAA flow/stage", "step16_NOAAflow.py"),
    ("Step 17: NOAA flows post-process", "step17_NOAAupdate.py"),
]

# ------------------------------------------------------------
# 1Y WINDOW TOGGLE
# ------------------------------------------------------------
# INCLUDE_1Y = True
INCLUDE_1Y = False  # uncomment to skip 1y window in steps 15/16


def extract_step_number(label: str) -> int:
    match = re.search(r"Step\s+(\d+)", label)
    if not match:
        raise ValueError(f"Cannot parse step number from label: {label}")
    return int(match.group(1))


def run_step(label: str, filename: str) -> None:
    path = CURRENT_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Step file missing: {path}")
    print(f"▶ {label}")
    runpy.run_path(str(path), run_name="__main__")
    print()


def iter_steps(start: int | None, end: int | None) -> list[tuple[int, str, str]]:
    filtered: list[tuple[int, str, str]] = []
    for label, filename in STEP_FILES:
        num = extract_step_number(label)
        if start is not None and num < start:
            continue
        if end is not None and num > end:
            continue
        filtered.append((num, label, filename))
    return filtered


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the DB-backed Flows pipeline steps.")
    parser.add_argument("--start", type=int, default=None, help="First step number to run (inclusive).")
    parser.add_argument("--end", type=int, default=None, help="Last step number to run (inclusive).")
    parser.add_argument("--list", action="store_true", help="List available steps and exit.")
    args = parser.parse_args()

    if args.list:
        for label, filename in STEP_FILES:
            print(f"{label} -> {filename}")
        return 0

    os.environ["FLOWS_INCLUDE_1Y"] = "1" if INCLUDE_1Y else "0"

    steps = iter_steps(args.start, args.end)
    if not steps:
        print("No steps selected. Check --start/--end.")
        return 1

    print("🌊🚀 Starting Flows Pipeline...\n")
    for _, label, filename in steps:
        try:
            run_step(label, filename)
        except Exception as exc:
            print(f"⚠️  {label} failed: {exc}")
            traceback.print_exc()
            print("🛑 Exiting Flows pipeline with code 0 to avoid immediate restart.")
            return 0

    print("🎉 Flows Pipeline finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
