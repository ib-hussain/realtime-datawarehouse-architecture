"""Validated, independently executable cleaning stage for dynamic imports.
Preserves the static pipeline's cleaning rules and counters.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data.static.clean_data import clean_file


def run(source: Path, output: Path) -> dict:
    if not source.is_file():
        raise FileNotFoundError(source)
    if source.resolve() == output.resolve():
        raise ValueError("Source and destination must differ")
    total, valid, duplicates, written = clean_file(source, output)
    return {
        "source": str(source),
        "output": str(output),
        "raw_rows": total,
        "valid_rows": valid,
        "duplicates_removed": duplicates,
        "clean_rows": written,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[2]
    p.add_argument("--input", type=Path, default=root / "data/raw/online-retail.csv")
    p.add_argument("--output", type=Path, default=root / "data/dynamic/processed-retail.csv")
    args = p.parse_args()
    print(json.dumps(run(args.input, args.output), indent=2))


if __name__ == "__main__":
    main()
