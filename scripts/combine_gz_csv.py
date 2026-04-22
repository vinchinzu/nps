#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import re
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


def natural_sort_key(path: Path) -> list[object]:
    parts = re.split(r"(\d+)", path.name)
    return [int(part) if part.isdigit() else part.lower() for part in parts]


def discover_inputs(pattern: str) -> list[Path]:
    matches = [path for path in SCRIPT_DIR.glob(pattern) if path.is_file()]
    return sorted(matches, key=natural_sort_key)


def normalize_header(line: str) -> str:
    return line if line.endswith("\n") else f"{line}\n"


def merge_gzip_csv(inputs: list[Path], output: Path) -> tuple[int, int]:
    header: str | None = None
    files_used = 0
    rows_written = 0

    with output.open("w", encoding="utf-8", newline="") as dst:
        for input_path in inputs:
            with gzip.open(input_path, "rt", encoding="utf-8", newline="") as src:
                try:
                    current_header = normalize_header(next(src))
                except StopIteration:
                    continue

                if header is None:
                    header = current_header
                    dst.write(header)
                elif current_header != header:
                    raise ValueError(
                        f"header mismatch in {input_path.name}; "
                        "only like-for-like CSV parts can be merged"
                    )

                for line in src:
                    dst.write(line)
                    rows_written += 1

            files_used += 1

    if header is None:
        raise ValueError("no readable CSV rows found in the discovered .gz files")

    return files_used, rows_written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge all discovered gzip-compressed CSV parts in the current directory."
    )
    parser.add_argument(
        "-o",
        "--output",
        default="combined.csv",
        help="output filename to create in the current directory (default: combined.csv)",
    )
    parser.add_argument(
        "--pattern",
        default="*.gz",
        help="glob pattern to discover input files in the current directory (default: *.gz)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace the output file if it already exists",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = SCRIPT_DIR / args.output

    if output.exists() and not args.overwrite:
        print(
            f"{output.name} already exists; rerun with --overwrite or choose --output",
            file=sys.stderr,
        )
        return 1

    inputs = discover_inputs(args.pattern)
    if not inputs:
        print(f"no files matched {args.pattern!r} in {SCRIPT_DIR}", file=sys.stderr)
        return 1

    try:
        files_used, rows_written = merge_gzip_csv(inputs, output)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(
        f"merged {files_used} files into {output.name} with {rows_written} data rows",
        file=sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
