"""DWBI CLI: prepare data, control MESHJOIN, inspect health, and run OLAP SQL."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from data.dynamic.clean_data import run as clean
from data.dynamic.data_split import run as split
from data.dynamic.feed_db import run as prepare
from meshjoin.base import load_runtime_config
from meshjoin.schema import apply_schema
from observatory import (
    ROOT,
    command,
    diagnostics,
    query,
    runtime_info,
    snapshot,
    start,
    stop,
    tail,
)

DEFAULT_RAW = ROOT / "data" / "raw" / "online-retail.csv"
DEFAULT_DYNAMIC = ROOT / "data" / "dynamic"
DEFAULT_SCHEMA = ROOT / "src" / "warehouse-schema.sql"
DEFAULT_QUERY = "SELECT COUNT(*) AS fact_rows FROM data_warehouse.transactions"


def _input_path(args: argparse.Namespace) -> Path:
    return (args.input_option or args.input_path).expanduser()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python cli_app.py doctor
  python cli_app.py clean
  python cli_app.py prepare --load-db
  python cli_app.py start --mode peak
  python cli_app.py control non-peak
  python cli_app.py stop
  python cli_app.py query "SELECT COUNT(*) FROM data_warehouse.transactions"

All file arguments have project defaults. Use --help after a command for details.
""",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show a traceback instead of the concise CLI error message.",
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    clean_parser = subparsers.add_parser("clean", help="Validate and deduplicate the raw CSV.")
    clean_parser.add_argument("input_path", nargs="?", type=Path, default=DEFAULT_RAW)
    clean_parser.add_argument("--input", dest="input_option", type=Path)
    clean_parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_DYNAMIC / "processed-retail.csv",
    )

    split_parser = subparsers.add_parser("split", help="Create online and periodic CSVs.")
    split_parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_DYNAMIC / "processed-retail.csv",
    )
    split_parser.add_argument("--output-dir", type=Path, default=DEFAULT_DYNAMIC)

    prepare_parser = subparsers.add_parser(
        "prepare",
        help="Run cleaning, splitting, master generation, and optional source-table loading.",
    )
    prepare_parser.add_argument("input_path", nargs="?", type=Path, default=DEFAULT_RAW)
    prepare_parser.add_argument("--input", dest="input_option", type=Path)
    prepare_parser.add_argument("--output-dir", type=Path, default=DEFAULT_DYNAMIC)
    prepare_parser.add_argument("--load-db", action="store_true")

    init_parser = subparsers.add_parser(
        "init-db",
        help="Destructively rebuild the five project schemas.",
    )
    init_parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    init_parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm that existing project-schema data may be dropped.",
    )

    start_parser = subparsers.add_parser("start", help="Launch the background MESHJOIN worker.")
    start_parser.add_argument("--mode", choices=("peak", "non-peak"), default="peak")
    start_parser.add_argument("--online-file", type=Path)

    control_parser = subparsers.add_parser("control", help="Send a command to the worker.")
    control_parser.add_argument(
        "value",
        choices=("online", "periodic", "both", "peak", "non-peak", "status", "quit"),
    )

    stop_parser = subparsers.add_parser("stop", help="Request and wait for graceful shutdown.")
    stop_parser.add_argument("--timeout", type=float, default=10.0)

    subparsers.add_parser("status", help="Show process and warehouse counters.")
    subparsers.add_parser("doctor", help="Run dependency, file, configuration, and DB checks.")

    logs_parser = subparsers.add_parser("logs", help="Print recent worker log lines.")
    logs_parser.add_argument("--lines", type=int, default=100)

    query_parser = subparsers.add_parser("query", help="Run one safe, read-only SELECT.")
    query_parser.add_argument("sql", nargs="?", default=DEFAULT_QUERY)
    query_parser.add_argument("--file", type=Path, help="Read SQL from a UTF-8 file.")
    query_parser.add_argument("--limit", type=int, default=100)
    return parser


def execute(args: argparse.Namespace) -> tuple[object | None, int]:
    if args.action == "clean":
        return clean(_input_path(args), args.output), 0
    if args.action == "split":
        return split(args.input, args.output_dir), 0
    if args.action == "prepare":
        database_config = load_runtime_config().database if args.load_db else None
        return prepare(_input_path(args), args.output_dir, database_config), 0
    if args.action == "init-db":
        if not args.yes:
            raise RuntimeError(
                "init-db drops and recreates all project schemas. Rerun with --yes "
                "only after verifying the target database and backups."
            )
        config = load_runtime_config().database
        count = apply_schema(config, args.schema)
        return {"schema": str(args.schema.resolve()), "statements_executed": count}, 0
    if args.action == "start":
        return {"pid": start(args.mode, args.online_file)}, 0
    if args.action == "control":
        command(args.value)
        return {"command": args.value, "accepted": True}, 0
    if args.action == "stop":
        if args.timeout <= 0:
            raise ValueError("--timeout must be positive.")
        stopped = stop(args.timeout)
        return {"stopped": stopped, "already_stopped": not stopped}, 0
    if args.action == "status":
        result: dict[str, object] = {"runtime": runtime_info()}
        try:
            result["warehouse"] = snapshot()
        except Exception as error:
            result["database_error"] = str(error)
        return result, 0
    if args.action == "doctor":
        result = diagnostics()
        return result, 0 if result["ok"] else 1
    if args.action == "logs":
        return tail(args.lines), 0

    sql = args.file.read_text(encoding="utf-8") if args.file else args.sql
    names, rows = query(sql, args.limit)
    return {"columns": names, "rows": rows, "row_count": len(rows)}, 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result, exit_code = execute(args)
        if isinstance(result, str):
            print(result, end="" if result.endswith("\n") or not result else "\n")
        elif result is not None:
            print(json.dumps(result, indent=2, default=str))
        return exit_code
    except Exception as error:
        if args.debug:
            raise
        detail = str(error).strip().splitlines()[0] if str(error).strip() else type(error).__name__
        print(f"ERROR: {detail}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
