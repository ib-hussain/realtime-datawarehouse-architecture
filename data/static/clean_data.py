"""Clean the raw Online Retail transaction file.

The project requires cleaning before the data is divided into online and
periodic streams. This script uses only the Python standard library.

Default input:  data/raw/online-retail.csv
Default output: data/static/processed-retail.csv
"""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal, InvalidOperation
from pathlib import Path


REQUIRED_COLUMNS = {
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
}


def project_root() -> Path:
    """Return the repository root, independent of the current directory."""

    return Path(__file__).resolve().parents[2]


def positive_integer(value: str | None) -> bool:
    try:
        return int((value or "").strip()) > 0
    except (TypeError, ValueError):
        return False


def positive_decimal(value: str | None) -> bool:
    try:
        number = Decimal((value or "").strip())
        return number.is_finite() and number > 0
    except (InvalidOperation, TypeError, ValueError):
        return False


def customer_id_present(value: str | None) -> bool:
    """Treat blank/null-like CSV values as missing customer IDs."""

    normalized = (value or "").strip().lower()
    return normalized not in {"", "nan", "none", "null"}


def is_valid_transaction(row: dict[str, str | None]) -> bool:
    invoice = (row.get("InvoiceNo") or "").strip()
    if invoice.startswith("C"):
        return False
    if not positive_integer(row.get("Quantity")):
        return False
    if not positive_decimal(row.get("UnitPrice")):
        return False
    return customer_id_present(row.get("CustomerID"))


def clean_file(input_path: Path, output_path: Path) -> tuple[int, int, int, int]:
    """Clean and deduplicate the input, returning audit counts.

    The duplicate key contains every source column exactly as read. Therefore,
    only exact duplicate rows are removed; rows differing in any field remain.
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    valid_rows = 0
    duplicate_rows = 0
    written_rows = 0
    seen_rows: set[tuple[str | None, ...]] = set()

    with input_path.open("r", encoding="cp1252", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("The input CSV has no header row.")

        missing = REQUIRED_COLUMNS.difference(reader.fieldnames)
        if missing:
            raise ValueError(
                "The input CSV is missing required columns: "
                + ", ".join(sorted(missing))
            )

        with output_path.open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(
                target,
                fieldnames=reader.fieldnames,
                extrasaction="ignore",
            )
            writer.writeheader()

            for row in reader:
                if not any(value not in (None, "") for value in row.values()):
                    continue
                total_rows += 1

                if not is_valid_transaction(row):
                    continue
                valid_rows += 1

                exact_key = tuple(row.get(column) for column in reader.fieldnames)
                if exact_key in seen_rows:
                    duplicate_rows += 1
                    continue

                seen_rows.add(exact_key)
                writer.writerow(row)
                written_rows += 1

    return total_rows, valid_rows, duplicate_rows, written_rows


def parse_args() -> argparse.Namespace:
    root = project_root()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=root / "data" / "raw" / "online-retail.csv",
        help="Raw CSV path (default: data/raw/online-retail.csv)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "data" / "static" / "processed-retail.csv",
        help="Cleaned CSV path (default: data/static/processed-retail.csv)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    total_rows, valid_rows, duplicate_rows, written_rows = clean_file(
        args.input, args.output
    )

    print(f"Rows read before cleaning: {total_rows}")
    print(f"Rows passing project cleaning rules: {valid_rows}")
    print(f"Exact duplicate rows removed: {duplicate_rows}")
    print(f"Rows written after cleaning: {written_rows}")
    print(f"Processed file: {args.output.resolve()}")


if __name__ == "__main__":
    main()

