"""Prepare project CSVs and populate the PostgreSQL input schemas.

This is the preparation stage of the project pipeline. It:

1. Cleans the raw file and removes exact duplicate rows.
2. Splits the cleaned rows using the required MD5 sales-mode rule.
3. Writes the project CSV files:
       online_sales_stream.csv
       customer_master_data.csv
       product_master_data.csv
4. Loads the two input schemas and the two staging master-data tables.

Run the schema SQL before running this file. Database credentials are accepted
as command-line arguments or requested interactively; passwords are never
written into this source file.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import shutil
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from getpass import getpass
from pathlib import Path

try:  # psycopg 3 is preferred.
    import psycopg  # type: ignore
except ImportError:  # pragma: no cover - depends on the user's environment.
    psycopg = None

try:  # psycopg2 is supported as a fallback.
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover - depends on the user's environment.
    psycopg2 = None

try:
    from .clean_data import clean_file
    from .data_split import split_file
except ImportError:  # Standalone execution
    from clean_data import clean_file
    from data_split import split_file


DATE_FORMAT = "%d/%m/%Y %H:%M"
INSERT_BATCH_SIZE = 2_000

GENDERS = ("Male", "Female")
AGE_GROUPS = ("0-17", "18-25", "26-35", "36-45", "46-50", "51-55", "55+")
OCCUPATIONS = (
    "Student",
    "Engineer",
    "Teacher",
    "Executive",
    "Clerk",
    "Healthcare",
    "Retail",
    "Homemaker",
    "Artist",
    "Other",
)
CITY_CATEGORIES = ("A", "B", "C")
STAY_VALUES = ("0", "1", "2", "3", "4+")
MARITAL_STATUSES = ("Single", "Married")
PRODUCT_CATEGORIES = (
    "Home Decor",
    "Kitchen",
    "Stationery",
    "Toys",
    "Seasonal Gifts",
    "Bags",
    "Jewelry",
    "Garden",
    "Party",
    "Other",
)
SUPPLIERS = (
    ("SUP01", "Northwind Supplies"),
    ("SUP02", "Atlantic Gifts Ltd"),
    ("SUP03", "Midlands Wholesale"),
    ("SUP04", "Celtic Craft Co"),
    ("SUP05", "EuroPack Partners"),
)


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def stable(text: str, modulus: int) -> int:
    """Implement the project's stable MD5(text, n) rule."""

    digest = hashlib.md5(text.encode("utf-8")).hexdigest()[:8]
    return int(digest, 16) % modulus


def parse_event_time(value: str) -> datetime:
    return datetime.strptime(value.strip(), DATE_FORMAT)


def integer_customer_id(value: str) -> int:
    # Customer IDs in the UCI file are whole numbers. This also gives a clear
    # error if a malformed value has somehow reached this stage.
    parsed = Decimal(value.strip())
    if parsed != parsed.to_integral_value():
        raise ValueError(f"CustomerID is not an integer: {value!r}")
    return int(parsed)


def prepare_master_rows(
    processed_path: Path,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Create deterministic customer and product master rows.

    The first country seen for a customer is retained. Product descriptions
    use the longest non-empty description, as required by the brief.
    """

    customer_countries: dict[int, str] = {}
    product_names: dict[str, str] = {}
    product_ids: set[str] = set()
    customer_ids: set[int] = set()

    with processed_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("The processed CSV has no header row.")

        required = {
            "InvoiceNo",
            "StockCode",
            "Description",
            "InvoiceDate",
            "CustomerID",
            "Country",
        }
        missing = required.difference(reader.fieldnames)
        if missing:
            raise ValueError(
                "The processed CSV is missing required columns: " + ", ".join(sorted(missing))
            )

        for row in reader:
            customer_id = integer_customer_id((row.get("CustomerID") or "").strip())
            product_id = (row.get("StockCode") or "").strip()
            country = (row.get("Country") or "").strip()
            description = (row.get("Description") or "").strip()

            customer_ids.add(customer_id)
            customer_countries.setdefault(customer_id, country)
            product_ids.add(product_id)

            old_name = product_names.get(product_id, "")
            if len(description) > len(old_name):
                product_names[product_id] = description

    customer_rows: list[dict[str, str]] = []
    for customer_id in sorted(customer_ids):
        customer_text = str(customer_id)
        country = customer_countries[customer_id]
        customer_rows.append(
            {
                "customer_id": customer_text,
                "gender": GENDERS[stable(customer_text, 2)],
                "age_group": AGE_GROUPS[stable(customer_text + "age", 7)],
                "occupation": OCCUPATIONS[stable(customer_text + "occ", 10)],
                "city_category": CITY_CATEGORIES[stable(customer_text + country, 3)],
                "stay_in_current_city_years": STAY_VALUES[stable(customer_text + "stay", 5)],
                "marital_status": MARITAL_STATUSES[stable(customer_text + "mar", 2)],
            }
        )

    product_rows: list[dict[str, str]] = []
    for product_id in sorted(product_ids):
        supplier_id, supplier_name = SUPPLIERS[stable(product_id, 5)]
        product_rows.append(
            {
                "product_id": product_id,
                "product_name": product_names.get(product_id) or "UNKNOWN",
                "product_category": PRODUCT_CATEGORIES[stable(product_id + "cat", 10)],
                "supplier_id": supplier_id,
                "supplier_name": supplier_name,
            }
        )

    return customer_rows, product_rows


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(rows)


def file_sha256(path: Path) -> str:
    """Return a streaming SHA-256 digest without loading the file into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_project_csvs(
    processed_path: Path,
    online_path: Path,
    online_submission_path: Path,
    customer_path: Path,
    product_path: Path,
) -> tuple[int, int]:
    """Write all CSV deliverables and return customer/product counts."""

    customer_columns = [
        "customer_id",
        "gender",
        "age_group",
        "occupation",
        "city_category",
        "stay_in_current_city_years",
        "marital_status",
    ]
    product_columns = [
        "product_id",
        "product_name",
        "product_category",
        "supplier_id",
        "supplier_name",
    ]

    customer_rows, product_rows = prepare_master_rows(processed_path)
    write_csv(customer_path, customer_columns, customer_rows)
    write_csv(product_path, product_columns, product_rows)

    # online.csv is generated by data_split.py. The submission name required by
    # the project is online_sales_stream.csv, so preserve both names.
    shutil.copyfile(online_path, online_submission_path)
    return len(customer_rows), len(product_rows)


def connect_database(config: DatabaseConfig):
    parameters = {
        "host": config.host,
        "port": config.port,
        "dbname": config.database,
        "user": config.user,
        "connect_timeout": 5,
        "application_name": "dwbi_preparation",
    }
    if config.password:
        parameters["password"] = config.password
    if psycopg is not None:
        return psycopg.connect(**parameters)
    if psycopg2 is not None:
        return psycopg2.connect(**parameters)
    raise RuntimeError("Install psycopg (recommended) or psycopg2 before loading PostgreSQL.")


def validate_preparation_schema(config: DatabaseConfig) -> None:
    """Fail before reading large CSVs when the target schema is incomplete."""

    required = (
        "oltp_online.online_sales_stream",
        "oltp_physical.periodic_sales_data",
        "staging_area.customer_master",
        "staging_area.product_master",
        "staging_area.load_manifest",
        "staging_area.store_list",
        "waiting_area.shed_table_physical",
        "data_warehouse.dim_customer",
        "data_warehouse.dim_product",
        "data_warehouse.dim_supplier",
        "data_warehouse.dim_store",
        "data_warehouse.dim_time",
        "data_warehouse.products_store",
        "data_warehouse.transactions",
        "data_warehouse.live_cache",
    )
    connection = connect_database(config)
    try:
        missing: list[str] = []
        with connection.cursor() as cursor:
            for relation in required:
                cursor.execute("SELECT to_regclass(%s)", (relation,))
                if cursor.fetchone()[0] is None:
                    missing.append(relation)
        if missing:
            raise RuntimeError(
                "The preparation schema is incomplete. Missing: "
                + ", ".join(missing)
                + ". Run `python cli_app.py init-db --yes` on the intended "
                "project database first."
            )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM data_warehouse.transactions),
                    (SELECT COUNT(*) FROM data_warehouse.live_cache),
                    (SELECT COUNT(*) FROM waiting_area.shed_table_physical)
                """
            )
            fact_rows, cache_rows, shed_rows = (int(value) for value in cursor.fetchone())
        if fact_rows or cache_rows or shed_rows:
            raise RuntimeError(
                "Refusing to replace source tables while warehouse runtime data exists "
                f"(facts={fact_rows}, cache={cache_rows}, shed={shed_rows}). Use a backup, "
                "then run `python cli_app.py init-db --yes` for an intentional clean replay."
            )
    finally:
        connection.close()


def chunks(rows: Sequence[tuple], size: int = INSERT_BATCH_SIZE):
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def insert_batches(connection, sql: str, rows: Sequence[tuple]) -> None:
    with connection.cursor() as cursor:
        for batch in chunks(rows):
            cursor.executemany(sql, batch)


def load_database(
    config: DatabaseConfig,
    online_csv: Path,
    periodic_csv: Path,
    customer_csv: Path,
    product_csv: Path,
) -> None:
    """Replace the preparation tables with the current deterministic data."""

    validate_preparation_schema(config)

    online_rows: list[tuple] = []
    with online_csv.open("r", encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            online_rows.append(
                (
                    row["transaction_id"],
                    parse_event_time(row["event_time"]),
                    integer_customer_id(row["customer_id"]),
                    row["product_id"],
                    int(row["quantity"]),
                    Decimal(row["unit_price"]),
                    row["sales_mode"].strip().lower() == "true",
                )
            )

    periodic_rows: list[tuple] = []
    with periodic_csv.open("r", encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            periodic_rows.append(
                (
                    row["transaction_id"],
                    parse_event_time(row["event_time"]),
                    integer_customer_id(row["customer_id"]),
                    row["product_id"],
                    int(row["quantity"]),
                    Decimal(row["unit_price"]),
                    row["sales_mode"].strip().lower() == "true",
                    int(row["release_hour"]),
                    row["released"].strip().lower() == "true",
                )
            )

    customer_rows: list[tuple] = []
    with customer_csv.open("r", encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            customer_rows.append(
                (
                    integer_customer_id(row["customer_id"]),
                    row["gender"],
                    row["age_group"],
                    row["occupation"],
                    row["city_category"],
                    row["stay_in_current_city_years"],
                    row["marital_status"],
                )
            )

    product_rows: list[tuple] = []
    with product_csv.open("r", encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            product_rows.append(
                (
                    row["product_id"],
                    row["product_name"],
                    row["product_category"],
                    row["supplier_id"],
                    row["supplier_name"],
                )
            )

    connection = connect_database(config)
    try:
        with connection.cursor() as cursor:
            # Preparation is rerunnable. Existing shed rows refer to periodic
            # rows, so CASCADE is intentional for this reset operation.
            cursor.execute(
                "TRUNCATE TABLE "
                "oltp_online.online_sales_stream, "
                "oltp_physical.periodic_sales_data, "
                "staging_area.customer_master, "
                "staging_area.product_master, "
                "staging_area.load_manifest "
                "RESTART IDENTITY CASCADE"
            )

        insert_batches(
            connection,
            """
            INSERT INTO oltp_online.online_sales_stream
                (transaction_id, event_time, customer_id, product_id,
                 quantity, unit_price, sales_mode)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            online_rows,
        )
        insert_batches(
            connection,
            """
            INSERT INTO oltp_physical.periodic_sales_data
                (transaction_id, event_time, customer_id, product_id,
                 quantity, unit_price, sales_mode, release_hour, released)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            periodic_rows,
        )
        insert_batches(
            connection,
            """
            INSERT INTO staging_area.customer_master
                (customer_id, gender, age_group, occupation, city_category,
                 stay_in_current_city_years, marital_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            customer_rows,
        )
        insert_batches(
            connection,
            """
            INSERT INTO staging_area.product_master
                (product_id, product_name, product_category,
                 supplier_id, supplier_name)
            VALUES (%s, %s, %s, %s, %s)
            """,
            product_rows,
        )
        manifest_rows = (
            (
                "online_sales_stream",
                str(online_csv.resolve()),
                len(online_rows),
                file_sha256(online_csv),
            ),
            (
                "periodic_sales_data",
                str(periodic_csv.resolve()),
                len(periodic_rows),
                file_sha256(periodic_csv),
            ),
            (
                "customer_master",
                str(customer_csv.resolve()),
                len(customer_rows),
                file_sha256(customer_csv),
            ),
            (
                "product_master",
                str(product_csv.resolve()),
                len(product_rows),
                file_sha256(product_csv),
            ),
        )
        insert_batches(
            connection,
            """
            INSERT INTO staging_area.load_manifest
                (dataset_name, source_path, row_count, sha256)
            VALUES (%s, %s, %s, %s)
            """,
            manifest_rows,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    print(f"Loaded online rows: {len(online_rows)}")
    print(f"Loaded periodic rows: {len(periodic_rows)}")
    print(f"Loaded customer master rows: {len(customer_rows)}")
    print(f"Loaded product master rows: {len(product_rows)}")


def environment_defaults() -> dict[str, str]:
    """Read root .env values while preserving process-environment overrides."""

    values = dict(os.environ)
    env_path = project_root() / ".env"
    if env_path.is_file():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return values


def database_config_from_args(args: argparse.Namespace) -> DatabaseConfig:
    values = environment_defaults()
    host = args.host or values.get("PGHOST", "localhost")
    port = args.port or int(values.get("PGPORT", "5432"))
    database = args.database or values.get("PGDATABASE", "realtime_data_stream")
    user = args.user or values.get("PGUSER", "postgres")
    password = args.password if args.password is not None else values.get("PGPASSWORD", "")
    if not password and sys.stdin.isatty():
        password = getpass("Database password (blank for peer/trust auth): ")
    return DatabaseConfig(host, port, database, user, password)


def parse_args() -> argparse.Namespace:
    root = project_root()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=root / "data" / "raw" / "online-retail.csv")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--database")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument(
        "--skip-db", action="store_true", help="Only create CSVs; do not connect to PostgreSQL."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = project_root()
    static = root / "data" / "static"
    processed = static / "processed-retail.csv"
    online = static / "online.csv"
    periodical = static / "periodical.csv"
    online_submission = static / "online_sales_stream.csv"
    customer_csv = static / "customer_master_data.csv"
    product_csv = static / "product_master_data.csv"

    total, valid, duplicates, written = clean_file(args.input, processed)
    online_count, periodic_count = split_file(processed, online, periodical)
    customer_count, product_count = write_project_csvs(
        processed,
        online,
        online_submission,
        customer_csv,
        product_csv,
    )

    print(f"Rows read before cleaning: {total}")
    print(f"Rows passing cleaning rules: {valid}")
    print(f"Exact duplicate rows removed: {duplicates}")
    print(f"Rows written after cleaning: {written}")
    print(f"Online rows prepared: {online_count}")
    print(f"Periodic rows prepared: {periodic_count}")
    print(f"Customer master CSV rows: {customer_count}")
    print(f"Product master CSV rows: {product_count}")

    if not args.skip_db:
        config = database_config_from_args(args)
        load_database(config, online_submission, periodical, customer_csv, product_csv)
    else:
        print("Database loading skipped (--skip-db).")


if __name__ == "__main__":
    main()
