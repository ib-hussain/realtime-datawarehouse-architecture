"""Split cleaned transactions into online and periodic project streams.

Classification is fixed by the project:

    int(MD5(transaction_id)[:8], 16) % 10

Remainders 0-6 are online; remainders 7-9 are periodic. Periodic rows are
sorted by event time and assigned to 48 balanced release groups (0-47).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from datetime import datetime
from pathlib import Path


ONLINE_COLUMNS = [
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "event_time",
    "sales_mode",
]

PERIODIC_COLUMNS = ONLINE_COLUMNS + ["release_hour", "released"]
GROUP_COUNT = 48
DATE_FORMAT = "%d/%m/%Y %H:%M"


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def parse_event_time(value: str, row_number: int) -> datetime:
    try:
        return datetime.strptime(value.strip(), DATE_FORMAT)
    except ValueError as error:
        raise ValueError(
            f"Invalid InvoiceDate at cleaned CSV row {row_number}: {value!r}"
        ) from error


def sales_mode(transaction_id: str) -> bool:
    digest_prefix = hashlib.md5(transaction_id.encode("utf-8")).hexdigest()[:8]
    remainder = int(digest_prefix, 16) % 10
    return remainder <= 6


def required_value(row: dict[str, str | None], column: str, row_number: int) -> str:
    value = row.get(column)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing {column} at cleaned CSV row {row_number}.")
    return value.strip()


def split_file(
    input_path: Path, online_path: Path, periodic_path: Path
) -> tuple[int, int]:
    online_rows: list[dict[str, str]] = []
    periodic_rows: list[tuple[datetime, int, dict[str, str]]] = []

    with input_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("The cleaned CSV has no header row.")

        expected_columns = {
            "InvoiceNo",
            "StockCode",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
        }
        missing = expected_columns.difference(reader.fieldnames)
        if missing:
            raise ValueError(
                "The cleaned CSV is missing required columns: "
                + ", ".join(sorted(missing))
            )

        for row_number, row in enumerate(reader, start=2):
            transaction_id = required_value(row, "InvoiceNo", row_number)
            event_time = required_value(row, "InvoiceDate", row_number)
            parsed_time = parse_event_time(event_time, row_number)
            common = {
                "transaction_id": transaction_id,
                "customer_id": required_value(row, "CustomerID", row_number),
                "product_id": required_value(row, "StockCode", row_number),
                "quantity": required_value(row, "Quantity", row_number),
                "unit_price": required_value(row, "UnitPrice", row_number),
                "event_time": event_time,
            }

            if sales_mode(transaction_id):
                online_rows.append({**common, "sales_mode": "true"})
            else:
                periodic_rows.append(
                    (parsed_time, row_number, {**common, "sales_mode": "false"})
                )

    # The original row number is a deterministic tie-breaker for equal times.
    periodic_rows.sort(key=lambda item: (item[0], item[1]))

    periodic_count = len(periodic_rows)
    online_path.parent.mkdir(parents=True, exist_ok=True)
    periodic_path.parent.mkdir(parents=True, exist_ok=True)

    with online_path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=ONLINE_COLUMNS)
        writer.writeheader()
        writer.writerows(online_rows)

    with periodic_path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=PERIODIC_COLUMNS)
        writer.writeheader()
        for index, (_, _, row) in enumerate(periodic_rows):
            # Group sizes differ by at most one row.
            release_hour = min(
                (index * GROUP_COUNT) // periodic_count,
                GROUP_COUNT - 1,
            ) if periodic_count else 0
            writer.writerow(
                {
                    **row,
                    "release_hour": release_hour,
                    "released": "false",
                }
            )

    return len(online_rows), periodic_count


def parse_args() -> argparse.Namespace:
    root = project_root()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=root / "data" / "static" / "processed-retail.csv",
        help="Cleaned CSV path (default: data/static/processed-retail.csv)",
    )
    parser.add_argument(
        "--online-output",
        type=Path,
        default=root / "data" / "static" / "online.csv",
        help="Online output path (default: data/static/online.csv)",
    )
    parser.add_argument(
        "--periodic-output",
        type=Path,
        default=root / "data" / "static" / "periodical.csv",
        help="Periodic output path (default: data/static/periodical.csv)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    online_count, periodic_count = split_file(
        args.input,
        args.online_output,
        args.periodic_output,
    )

    print(f"Online rows written: {online_count}")
    print(f"Periodic rows written: {periodic_count}")
    print(f"Periodic release groups: {GROUP_COUNT}")
    print(f"Online file: {args.online_output.resolve()}")
    print(f"Periodic file: {args.periodic_output.resolve()}")


if __name__ == "__main__":
    main()
