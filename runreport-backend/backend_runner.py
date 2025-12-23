"""
backend_runner.py
-----------------------------------------
RunReport backend "meta-runner".

Runs the three backend pipelines in order by invoking each folder's
`step0_runner.py`:
  1) Columbia_FishCounts
  2) EscapementReport_FishCounts
  3) Flows

Usage:
    python3 backend_runner.py
    python3 backend_runner.py --only columbia
    python3 backend_runner.py --skip escapement --skip flows
"""

# The tables to plot are:
# EscapementRepot_PlotData, Columbia_FishCounts, NOAA_flows, USGS_flows

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from publish.publisher import publish_all


BACKEND_ROOT = Path(__file__).resolve().parent
ESCAPEMENT_SIGNAL_PATH = BACKEND_ROOT / ".escapement_new_pdfs"

PIPELINES: dict[str, Path] = {
    "columbia": BACKEND_ROOT / "Columbia_FishCounts" / "step0_runner.py",
    "escapement": BACKEND_ROOT / "EscapementReport_FishCounts" / "step0_runner.py",
    "flows": BACKEND_ROOT / "Flows" / "step0_runner.py",
}


def run_step0(name: str, script_path: Path) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"Missing step0 runner for '{name}': {script_path}")

    print(f"\n==================== {name.upper()} ====================\n")
    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(script_path.parent),
        check=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run all RunReport backend pipelines in order.")
    parser.add_argument(
        "--only",
        choices=sorted(PIPELINES.keys()),
        help="Run a single pipeline and exit.",
    )
    parser.add_argument(
        "--skip",
        action="append",
        choices=sorted(PIPELINES.keys()),
        default=[],
        help="Skip a pipeline (can be provided multiple times).",
    )
    return parser.parse_args()


def build_publish_flags(args: argparse.Namespace) -> dict[str, bool]:
    flags = {name: False for name in PIPELINES}
    if args.only:
        flags[args.only] = True
        return flags

    skips = set(args.skip or [])
    for name in PIPELINES:
        flags[name] = name not in skips
    return flags


def apply_escapement_publish_signal(flags: dict[str, bool]) -> None:
    if not flags.get("escapement"):
        return
    if not ESCAPEMENT_SIGNAL_PATH.exists():
        return
    signal = ESCAPEMENT_SIGNAL_PATH.read_text().strip().lower()
    if signal == "0":
        print("⏭️  Escapement publish skipped: no new PDFs found.")
        flags["escapement"] = False


def main() -> int:
    args = parse_args()
    flags = build_publish_flags(args)

    if args.only:
        run_step0(args.only, PIPELINES[args.only])
        apply_escapement_publish_signal(flags)
        publish_all(flags)
        return 0

    skips = set(args.skip or [])
    for name in ("columbia", "escapement", "flows"):
        if name in skips:
            print(f"\n[skip] {name}")
            continue
        run_step0(name, PIPELINES[name])

    apply_escapement_publish_signal(flags)
    publish_all(flags)
    print("\n✅ All selected backend pipelines finished.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
