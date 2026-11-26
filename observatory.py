"""Shared PostgreSQL observation, diagnostics, and runtime process control."""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
import uuid
from collections import deque
from datetime import UTC, datetime
from pathlib import Path

from meshjoin.base import connect_database, load_runtime_config
from meshjoin.schema import RUNTIME_RELATIONS, relation_status, require_relations

ROOT = Path(__file__).resolve().parent
RUN_DIR = ROOT / "logs"
PID_PATH = RUN_DIR / "runtime.json"
CONTROL = RUN_DIR / "control.json"
TELEMETRY = RUN_DIR / "telemetry.json"
LOG = RUN_DIR / "meshjoin.log"

SNAPSHOT_STATEMENTS = {
    "fact_rows": (
        "data_warehouse.transactions",
        "SELECT COUNT(*) FROM data_warehouse.transactions",
    ),
    "revenue": (
        "data_warehouse.transactions",
        "SELECT COALESCE(SUM(revenue), 0) FROM data_warehouse.transactions",
    ),
    "online_rows": (
        "data_warehouse.transactions",
        "SELECT COUNT(*) FROM data_warehouse.transactions WHERE sales_mode",
    ),
    "periodic_rows": (
        "data_warehouse.transactions",
        "SELECT COUNT(*) FROM data_warehouse.transactions WHERE NOT sales_mode",
    ),
    "cached_rows": (
        "data_warehouse.live_cache",
        "SELECT COUNT(*) FROM data_warehouse.live_cache",
    ),
    "shed_rows": (
        "waiting_area.shed_table_physical",
        "SELECT COUNT(*) FROM waiting_area.shed_table_physical WHERE NOT processed",
    ),
    "online_source": (
        "oltp_online.online_sales_stream",
        "SELECT COUNT(*) FROM oltp_online.online_sales_stream",
    ),
    "periodic_source": (
        "oltp_physical.periodic_sales_data",
        "SELECT COUNT(*) FROM oltp_physical.periodic_sales_data",
    ),
    "customer_master": (
        "staging_area.customer_master",
        "SELECT COUNT(*) FROM staging_area.customer_master",
    ),
    "product_master": (
        "staging_area.product_master",
        "SELECT COUNT(*) FROM staging_area.product_master",
    ),
}

BLOCKED_SQL = re.compile(
    r"\b(?:insert|update|delete|merge|create|alter|drop|truncate|grant|revoke|"
    r"comment|reindex|refresh|copy|do|call|vacuum|analyze|listen|notify|lock|set|reset|"
    r"for\s+update|for\s+share|into\s+[a-z_]|pg_sleep|pg_read_file|"
    r"pg_read_binary_file|pg_ls_dir|pg_stat_file|pg_terminate_backend|"
    r"pg_cancel_backend|pg_advisory_[a-z_]+|pg_[a-z_]+\s*\(|set_config|"
    r"nextval|setval|dblink|lo_import|lo_export|public\s*\.|pg_catalog\s*\.|"
    r"information_schema\s*\.)\b",
    re.IGNORECASE,
)


def database():
    return connect_database(load_runtime_config().database)


def query(sql: str, limit: int = 500) -> tuple[list[str], list[tuple]]:
    """Run one bounded, read-only SELECT for the local query explorer."""

    try:
        import sqlparse
    except ImportError as error:  # pragma: no cover - dependency check path.
        raise RuntimeError("Install project requirements before running SQL queries.") from error

    sql = sql.strip()
    if not sql:
        raise ValueError("SQL cannot be empty.")
    statements = sqlparse.parse(sql)
    if len(statements) != 1 or statements[0].get_type() != "SELECT":
        raise ValueError("Only one SELECT statement is permitted.")
    without_comments = sqlparse.format(sql, strip_comments=True)
    normalized_sql = re.sub(r"\s*\.\s*", ".", without_comments.replace('"', ""))
    if BLOCKED_SQL.search(normalized_sql):
        raise ValueError("The query contains an operation disabled in the read-only explorer.")
    if not 1 <= limit <= 2_000:
        raise ValueError("Limit must be between 1 and 2,000.")

    connection = database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5000ms'")
            cursor.execute(sql)
            names = [description[0] for description in cursor.description]
            rows = cursor.fetchmany(limit + 1)
        connection.rollback()
        return names, rows[:limit]
    finally:
        connection.close()


def snapshot() -> dict:
    """Return live project counts without crashing on a partially applied schema."""

    connection = database()
    try:
        relations = relation_status(connection, RUNTIME_RELATIONS)
        result: dict[str, object] = {}
        with connection.cursor() as cursor:
            for key, (relation, sql) in SNAPSHOT_STATEMENTS.items():
                result[key] = scalar(cursor, sql) if relations.get(relation, False) else 0
        missing = [name for name, exists in relations.items() if not exists]
        result["schema_ready"] = not missing
        result["missing_relations"] = missing
        return result
    finally:
        connection.close()


def scalar(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchone()[0]


def _read_pid_record() -> dict | None:
    if not PID_PATH.exists():
        return None
    try:
        payload = json.loads(PID_PATH.read_text(encoding="utf-8"))
        payload["pid"] = int(payload["pid"])
        return payload
    except (OSError, ValueError, KeyError, json.JSONDecodeError):
        PID_PATH.unlink(missing_ok=True)
        return None


def _windows_pid_is_alive(pid: int) -> bool:
    """Check a process handle on Windows; ``os.kill(pid, 0)`` is unreliable there."""

    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    open_process.restype = wintypes.HANDLE
    handle = open_process(0x00100000 | 0x1000, False, pid)  # SYNCHRONIZE | QUERY_LIMITED_INFO
    if not handle:
        return False
    try:
        exit_code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return False
        return exit_code.value == 259  # STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def is_running() -> bool:
    payload = _read_pid_record()
    if payload is None:
        return False
    pid = payload["pid"]
    try:
        if os.name == "nt":
            if not _windows_pid_is_alive(pid):
                PID_PATH.unlink(missing_ok=True)
                return False
        else:
            os.kill(pid, 0)
        process_command = Path(f"/proc/{pid}/cmdline")
        if process_command.exists() and b"data_enrichment" not in process_command.read_bytes():
            PID_PATH.unlink(missing_ok=True)
            return False
        return True
    except OSError:
        PID_PATH.unlink(missing_ok=True)
        return False


def runtime_info() -> dict:
    running = is_running()
    payload = _read_pid_record() or {}
    telemetry = None
    if TELEMETRY.is_file():
        try:
            value = json.loads(TELEMETRY.read_text(encoding="utf-8"))
            telemetry = value if isinstance(value, dict) else None
        except (OSError, json.JSONDecodeError):
            telemetry = None
    return {**payload, "running": running, "telemetry": telemetry}


def runtime_preflight(online_file: str | Path | None = None) -> dict:
    """Validate files, schema, and staged master data before launching."""

    runtime = load_runtime_config()
    source = Path(online_file) if online_file is not None else runtime.online_file
    if not source.is_absolute():
        source = ROOT / source
    source = source.resolve()
    if not source.is_file():
        raise FileNotFoundError(
            f"Online stream file not found: {source}. Run `python cli_app.py prepare` first."
        )

    require_relations(runtime.database)
    current = snapshot()
    empty = [
        label
        for label in ("online_source", "periodic_source", "customer_master", "product_master")
        if int(current[label]) == 0
    ]
    if empty:
        raise RuntimeError(
            "Runtime inputs are empty: " + ", ".join(empty) + ". Run "
            "`python cli_app.py prepare --load-db` before starting the worker."
        )
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required_columns = {
            "transaction_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "event_time",
            "sales_mode",
        }
        missing_columns = required_columns.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                "Online stream CSV is missing columns: " + ", ".join(sorted(missing_columns))
            )
        source_rows = sum(1 for _ in reader)
    if source_rows != int(current["online_source"]):
        raise RuntimeError(
            f"Online CSV contains {source_rows:,} rows but PostgreSQL contains "
            f"{int(current['online_source']):,}. Prepare the selected source and database "
            "together before starting."
        )

    digest = hashlib.sha256()
    with source.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    source_sha256 = digest.hexdigest()

    connection = database()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT row_count, sha256
                FROM staging_area.load_manifest
                WHERE dataset_name = 'online_sales_stream'
                """
            )
            manifest = cursor.fetchone()
    finally:
        connection.close()
    if manifest is None:
        raise RuntimeError(
            "The preparation manifest has no online source entry. Run "
            "`python cli_app.py prepare --load-db` before starting."
        )
    manifest_rows, manifest_sha256 = int(manifest[0]), str(manifest[1]).strip()
    if manifest_rows != source_rows or manifest_sha256 != source_sha256:
        raise RuntimeError(
            "The selected online CSV is not the file loaded into PostgreSQL. "
            "Prepare the selected source and database together before starting."
        )
    return {
        "online_file": str(source),
        "online_rows": source_rows,
        "online_sha256": source_sha256,
        "snapshot": current,
    }


def start(mode: str = "peak", online_file: str | Path | None = None) -> int:
    if mode not in {"peak", "non-peak"}:
        raise ValueError("Mode must be 'peak' or 'non-peak'.")
    if is_running():
        raise RuntimeError("Runtime is already running.")

    preflight = runtime_preflight(online_file)
    source = Path(preflight["online_file"])
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    CONTROL.unlink(missing_ok=True)
    TELEMETRY.unlink(missing_ok=True)
    child_command = [
        sys.executable,
        "-u",
        str(ROOT / "data_enrichment.py"),
        "--no-console",
        "--arrival-mode",
        mode,
        "--control-file",
        str(CONTROL),
        "--telemetry-file",
        str(TELEMETRY),
        "--online-file",
        str(source),
    ]
    started_at = datetime.now(UTC).isoformat()
    with LOG.open("a", encoding="utf-8") as logfile:
        logfile.write(f"\n=== runtime launch {started_at} ({mode}) ===\n")
        logfile.flush()
        process = subprocess.Popen(
            child_command,
            cwd=ROOT,
            stdout=logfile,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    PID_PATH.write_text(
        json.dumps(
            {
                "pid": process.pid,
                "started_at": started_at,
                "mode": mode,
                "online_file": str(source),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    for _ in range(150):
        time.sleep(0.1)
        return_code = process.poll()
        if return_code is not None:
            PID_PATH.unlink(missing_ok=True)
            recent = tail(20).strip()
            detail = f"\n{recent}" if recent else ""
            raise RuntimeError(f"Runtime exited during startup with code {return_code}.{detail}")
        if TELEMETRY.is_file():
            try:
                telemetry = json.loads(TELEMETRY.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            errors = [message for message in telemetry.get("errors", {}).values() if message]
            if errors:
                process.terminate()
                process.wait(timeout=5)
                PID_PATH.unlink(missing_ok=True)
                raise RuntimeError("Runtime failed during startup: " + "; ".join(errors))
            return process.pid

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    PID_PATH.unlink(missing_ok=True)
    recent = tail(20).strip()
    detail = f"\n{recent}" if recent else ""
    raise RuntimeError(f"Runtime did not publish healthy telemetry within 15 seconds.{detail}")


def command(value: str) -> None:
    if value not in {"online", "periodic", "both", "peak", "non-peak", "status", "quit"}:
        raise ValueError("Invalid runtime command.")
    if not is_running():
        raise RuntimeError("Runtime is not running.")
    temporary = CONTROL.with_suffix(".tmp")
    temporary.write_text(
        json.dumps({"id": str(uuid.uuid4()), "command": value}),
        encoding="utf-8",
    )
    temporary.replace(CONTROL)


def stop(timeout: float = 10.0) -> bool:
    """Request graceful shutdown and wait for the worker to exit."""

    if not is_running():
        return False
    command("quit")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not is_running():
            PID_PATH.unlink(missing_ok=True)
            return True
        time.sleep(0.2)
    raise TimeoutError("Runtime did not stop within the requested timeout.")


def tail(lines: int = 60) -> str:
    if not 1 <= lines <= 2_000:
        raise ValueError("Log line count must be between 1 and 2,000.")
    if not LOG.exists():
        return ""
    with LOG.open(encoding="utf-8", errors="replace") as handle:
        return "".join(deque(handle, maxlen=lines))


def diagnostics() -> dict:
    """Run bounded, non-mutating checks used by the CLI and health page."""

    checks: list[dict[str, str]] = []

    def add(name: str, status: str, detail: str) -> None:
        checks.append({"check": name, "status": status, "detail": detail})

    for module in ("pandas", "streamlit", "sqlparse"):
        found = importlib.util.find_spec(module) is not None
        add(
            f"Python dependency: {module}",
            "ok" if found else "error",
            "Installed" if found else "Missing; run `python -m pip install -r requirements.txt`.",
        )
    driver = (
        "psycopg"
        if importlib.util.find_spec("psycopg")
        else ("psycopg2" if importlib.util.find_spec("psycopg2") else None)
    )
    add(
        "PostgreSQL driver",
        "ok" if driver else "error",
        driver or "Missing; install psycopg[binary].",
    )

    try:
        runtime = load_runtime_config()
        add(
            "Configuration",
            "ok",
            f"{runtime.database.host}:{runtime.database.port}/{runtime.database.database} "
            f"as {runtime.database.user}",
        )
    except Exception as error:
        add("Configuration", "error", str(error))
        runtime = None

    schema_file = ROOT / "src" / "warehouse-schema.sql"
    add("Schema file", "ok" if schema_file.is_file() else "error", str(schema_file))
    raw_file = ROOT / "data" / "raw" / "online-retail.csv"
    add(
        "Raw source CSV",
        "ok" if raw_file.is_file() else "warning",
        str(raw_file)
        if raw_file.is_file()
        else "Not found; existing prepared CSVs may still be used.",
    )

    current_snapshot = None
    if runtime is not None and driver:
        add(
            "Configured online stream",
            "ok" if runtime.online_file.is_file() else "error",
            str(runtime.online_file),
        )
        try:
            current_snapshot = snapshot()
            missing = current_snapshot["missing_relations"]
            add("Database connection", "ok", "Connected successfully.")
            add(
                "Project schema",
                "ok" if not missing else "error",
                "All runtime relations are present."
                if not missing
                else "Missing: " + ", ".join(missing),
            )
            empty = [
                name
                for name in (
                    "online_source",
                    "periodic_source",
                    "customer_master",
                    "product_master",
                )
                if int(current_snapshot[name]) == 0
            ]
            add(
                "Prepared inputs",
                "ok" if not empty else "warning",
                "Source and master tables contain data."
                if not empty
                else "Empty: " + ", ".join(empty),
            )
        except Exception as error:
            add("Database connection", "error", str(error))

    running = is_running()
    add(
        "MESHJOIN worker",
        "ok" if running else "warning",
        "Running" if running else "Stopped",
    )
    return {
        "ok": not any(check["status"] == "error" for check in checks),
        "checks": checks,
        "snapshot": current_snapshot,
        "runtime": runtime_info(),
    }
