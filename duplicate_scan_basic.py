#!/usr/bin/env python3
"""Basic duplicate scanner using full-file MD5 hashing (single-threaded).

Usage:
    python duplicate_scan_basic.py /path/to/root
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Iterator

CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
REPORT_NAME = "duplicate_report_duplicate_scan_basic.json"

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


def hash_file(path: Path) -> str:
    """Compute full MD5 hash for a file."""
    digest = hashlib.md5()

    with path.open("rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)

    return digest.hexdigest()


def write_report(report_path: Path, duplicate_groups: list[dict[str, object]]) -> None:
    payload = {"duplicate_groups": duplicate_groups}
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Basic full-hash duplicate scanner")
    parser.add_argument("root", type=Path, help="Root directory to scan recursively")
    args = parser.parse_args()

    root = args.root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"Invalid root directory: {root}")
        return 1

    start = time.perf_counter()

    hash_to_files: DefaultDict[str, list[str]] = defaultdict(list)
    total_files = 0

    for file_path in iter_files(root):
        total_files += 1
        try:
            file_hash = hash_file(file_path)
            hash_to_files[file_hash].append(str(file_path.resolve()))
        except (PermissionError, OSError):
            continue

    duplicate_groups = [
        {"hash": file_hash, "files": paths}
        for file_hash, paths in hash_to_files.items()
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
    print(f"Execution time: {elapsed:.6f} seconds")
    print(f"Report saved to: {report_path.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
