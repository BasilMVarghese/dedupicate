#!/usr/bin/env python3
"""Benchmark runner for duplicate scanning methods.

Usage:
    python benchmark_duplicates.py /path/to/root
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

METHODS: list[tuple[str, str]] = [
    ("Basic hashing", "duplicate_scan_basic.py"),
    ("Multithread hashing", "duplicate_scan_multithread.py"),
    ("Partial hashing", "duplicate_scan_partial_hash.py"),
    ("Size + partial + full hashing", "duplicate_scan_optimized.py"),
]


def run_script(script_path: Path, root: Path) -> tuple[float | None, int]:
    """Run one scanner and return (elapsed_seconds_or_none, exit_code)."""
    start = time.perf_counter()
    result = subprocess.run([sys.executable, str(script_path), str(root)])
    elapsed = time.perf_counter() - start

    if result.returncode != 0:
        return None, result.returncode
    return elapsed, 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark duplicate scanning scripts")
    parser.add_argument("root", type=Path, help="Root directory to scan recursively")
    args = parser.parse_args()

    root = args.root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"Invalid root directory: {root}")
        return 1

    scripts_dir = Path(__file__).resolve().parent
    results: list[tuple[str, float | None]] = []
    failures = 0

    for method_name, script_name in METHODS:
        script_path = scripts_dir / script_name
        if not script_path.exists():
            print(f"Skipping {method_name}: missing script {script_path}")
            results.append((method_name, None))
            failures += 1
            continue

        print(f"\nRunning: {method_name}")
        elapsed, return_code = run_script(script_path, root)
        results.append((method_name, elapsed))
        if return_code != 0:
            print(f"{method_name} failed with exit code {return_code}")
            failures += 1

    print("\nMethod                         Time (seconds)")
    print("------------------------------------------------")
    for method_name, elapsed in results:
        value = f"{elapsed:.2f}" if elapsed is not None else "FAILED"
        print(f"{method_name:<30} {value}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
