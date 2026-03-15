#!/usr/bin/env python3
"""Duplicate scanner using partial hash prefilter and full SHA256 confirmation.

Usage:
    python duplicate_scan_partial_hash.py /path/to/root
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import DefaultDict, Iterator

CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
PARTIAL_BYTES = 1 * 1024 * 1024  # 1MB
REPORT_NAME = "duplicate_report_duplicate_scan_partial_hash.json"

# Only scan images, videos, and PDFs.
SUPPORTED_EXTENSIONS: set[str] = {
    # Images
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif",
    ".webp", ".svg", ".ico", ".heic", ".heif", ".raw", ".cr2",
    ".nef", ".arw", ".dng", ".orf", ".rw2", ".psd",
    # Videos
    ".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".3gp", ".ts", ".vob", ".mts",
    ".m2ts", ".ogv",
    # PDFs
    ".pdf",
}


def iter_files(root: Path) -> Iterator[Path]:
    """Yield supported files recursively, skipping symlinks and errors."""
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            for entry in current.iterdir():
                try:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir():
                        stack.append(entry)
                    elif entry.is_file() and entry.suffix.lower() in SUPPORTED_EXTENSIONS:
                        yield entry
                except (PermissionError, OSError):
                    continue
        except (PermissionError, OSError):
            continue


def sha256_file(path: Path, limit_bytes: int | None = None) -> str:
    """Compute SHA256 for a file, optionally limiting bytes read."""
    digest = hashlib.sha256()
    remaining = limit_bytes

    with path.open("rb") as f:
        while True:
            if remaining is None:
                chunk = f.read(CHUNK_SIZE)
            else:
                if remaining <= 0:
                    break
                chunk = f.read(min(CHUNK_SIZE, remaining))
                remaining -= len(chunk)

            if not chunk:
                break
            digest.update(chunk)

    return digest.hexdigest()


def write_report(report_path: Path, duplicate_groups: list[dict[str, object]]) -> None:
    payload = {"duplicate_groups": duplicate_groups}
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _hash_safe(path: Path, limit_bytes: int | None = None) -> tuple[Path, str | None]:
    """Hash wrapper that returns None on errors (thread-safe)."""
    try:
        return path, sha256_file(path, limit_bytes=limit_bytes)
    except (PermissionError, OSError):
        return path, None


def main() -> int:
    parser = argparse.ArgumentParser(description="Partial-hash duplicate scanner (multithreaded)")
    parser.add_argument("root", type=Path, help="Root directory to scan recursively")
    args = parser.parse_args()

    root = args.root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"Invalid root directory: {root}")
        return 1

    workers = min(32, (os.cpu_count() or 1) * 4)
    start = time.perf_counter()

    file_paths = list(iter_files(root))
    total_files = len(file_paths)

    # Stage 1: partial hash for all files (multithreaded).
    partial_map: DefaultDict[str, list[Path]] = defaultdict(list)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(_hash_safe, fp, PARTIAL_BYTES) for fp in file_paths
        ]
        for future in as_completed(futures):
            path, part_hash = future.result()
            if part_hash is not None:
                partial_map[part_hash].append(path)

    # Collect candidates that share a partial hash.
    candidates = [
        fp for group in partial_map.values() if len(group) > 1 for fp in group
    ]

    # Stage 2: full hash only for partial collisions (multithreaded).
    full_map: DefaultDict[str, list[str]] = defaultdict(list)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_hash_safe, fp) for fp in candidates]
        for future in as_completed(futures):
            path, full_hash = future.result()
            if full_hash is not None:
                full_map[full_hash].append(str(path.resolve()))

    duplicate_groups = [
        {"hash": file_hash, "files": paths}
        for file_hash, paths in full_map.items()
        if len(paths) > 1
    ]

    duplicate_files = sum(len(group["files"]) - 1 for group in duplicate_groups)
    elapsed = time.perf_counter() - start

    report_path = Path.cwd() / REPORT_NAME
    write_report(report_path, duplicate_groups)

    print("Scan completed")
    print(f"Total files scanned: {total_files}")
    print(f"Duplicate groups: {len(duplicate_groups)}")
    print(f"Duplicate files: {duplicate_files}")
    print(f"Execution time: {elapsed:.2f} seconds")
    print(f"Report saved to: {report_path.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
