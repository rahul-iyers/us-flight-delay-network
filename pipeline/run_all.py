"""
Run the entire data pipeline in order.

Usage:
    python pipeline/run_all.py [--skip-step N]

Steps:
  1  load_clean      — 01_load_clean.py
  2  aggregate       — 02_aggregate.py
  3  network         — 03_network.py
  4  propagation     — 04_propagation.py
"""

import sys
import time
import argparse
import importlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

STEPS = [
    ("01_load_clean",  "load_clean",   "01_load_clean"),
    ("02_aggregate",   "aggregate",    "02_aggregate"),
    ("03_network",     "network",      "03_network"),
    ("04_propagation", "propagation",  "04_propagation"),
]


def run_step(module_name: str) -> None:
    mod = importlib.import_module(module_name)
    mod.main()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full flight-delay pipeline.")
    parser.add_argument(
        "--skip", nargs="*", default=[],
        help="Step numbers to skip, e.g. --skip 1 4"
    )
    parser.add_argument(
        "--only", nargs="*", default=[],
        help="Run only these step numbers, e.g. --only 2 3"
    )
    args = parser.parse_args()

    skip_set = set(int(s) for s in args.skip)
    only_set = set(int(s) for s in args.only) if args.only else None

    t_total = time.time()
    for i, (module_name, label, _) in enumerate(STEPS, start=1):
        if i in skip_set:
            print(f"\n{'='*60}")
            print(f"STEP {i}: {label.upper()} — SKIPPED")
            continue
        if only_set and i not in only_set:
            continue

        print(f"\n{'='*60}")
        print(f"STEP {i}/{len(STEPS)}: {label.upper()}")
        print("="*60)
        t0 = time.time()
        try:
            run_step(module_name)
        except Exception as exc:
            print(f"\n[ERROR] Step {i} failed: {exc}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        elapsed = time.time() - t0
        print(f"\nStep {i} completed in {elapsed:.1f}s")

    total = time.time() - t_total
    print(f"\n{'='*60}")
    print(f"ALL STEPS COMPLETE  — total time: {total:.1f}s")


if __name__ == "__main__":
    main()
