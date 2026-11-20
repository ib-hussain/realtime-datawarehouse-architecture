"""Dynamic pipeline for an explicitly selected UCI-format source CSV.

Uses the canonical static rules for master-data derivation and PostgreSQL inserts.
Warning: this is an initial-seed/reseed operation, not an append-only CDC ingestor.
It must not be run while the MESHJOIN worker is processing the same database.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data.dynamic.clean_data import run as clean
from data.dynamic.data_split import run as split
from data.static.feed_db import DatabaseConfig, load_database, write_project_csvs
from meshjoin.base import load_runtime_config


def run(source: Path, output_dir: Path, config: DatabaseConfig | None = None) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    processed = output_dir / "processed-retail.csv"
    metrics = clean(source, processed)
    metrics.update(split(processed, output_dir))
    customer = output_dir / "customer_master_data.csv"
    product = output_dir / "product_master_data.csv"
    online_feed = output_dir / "online_sales_stream.csv"
    customer_n, product_n = write_project_csvs(
        processed, output_dir / "online.csv", online_feed, customer, product
    )
    metrics.update(
        {
            "customer_master_rows": customer_n,
            "product_master_rows": product_n,
            "database_loaded": False,
        }
    )
    if config is not None:
        load_database(config, online_feed, output_dir / "periodical.csv", customer, product)
        metrics["database_loaded"] = True
    (output_dir / "last_run.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return metrics


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[2]
    p.add_argument("--input", type=Path, default=root / "data/raw/online-retail.csv")
    p.add_argument("--output-dir", type=Path, default=root / "data/dynamic")
    p.add_argument("--load-db", action="store_true")
    p.add_argument("--host")
    p.add_argument("--port", type=int)
    p.add_argument("--database")
    p.add_argument("--user")
    p.add_argument("--password")
    a = p.parse_args()
    config = None
    if a.load_db:
        import sys
        from getpass import getpass

        defaults = load_runtime_config().database
        password = a.password if a.password is not None else defaults.password
        if not password and sys.stdin.isatty():
            password = getpass("PostgreSQL password (blank for peer/trust auth): ")
        config = DatabaseConfig(
            a.host or defaults.host,
            a.port or defaults.port,
            a.database or defaults.database,
            a.user or defaults.user,
            password,
        )
    print(json.dumps(run(a.input, a.output_dir, config), indent=2))


if __name__ == "__main__":
    main()
