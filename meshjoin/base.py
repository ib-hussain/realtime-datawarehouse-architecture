"""Shared configuration and PostgreSQL connection helpers.

The project is intended to run locally with very little configuration.  Safe,
non-secret defaults are therefore kept here while environment variables (or a
root ``.env`` file) remain the source of credentials and overrides.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    import psycopg  # type: ignore
except ImportError:  # pragma: no cover - depends on the local machine.
    psycopg = None

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover - depends on the local machine.
    psycopg2 = None


@dataclass(frozen=True)
class DatabaseConfig:
    """Credentials and network location for the PostgreSQL database."""

    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class RuntimeConfig:
    """Database connection and tunable runtime parameters."""

    database: DatabaseConfig
    online_file: Path
    master_partition_size: int = 500
    stream_chunk_size: int = 500
    stream_buffer_limit: int = 1_000
    peak_rate: int = 80
    non_peak_rate: int = 10
    simulated_hour_seconds: int = 60
    periodic_group_count: int = 48


DEFAULT_ENV = {
    "PGHOST": "localhost",
    "PGPORT": "5432",
    "PGDATABASE": "realtime_data_stream",
    "PGUSER": "postgres",
    "PGPASSWORD": "",
    "ONLINE_SALES_FILE": "data/static/online_sales_stream.csv",
    "MD_PARTITION_SIZE": "500",
    "STREAM_CHUNK_SIZE": "500",
    "STREAM_BUFFER_LIMIT": "1000",
    "PEAK_RATE": "80",
    "NON_PEAK_RATE": "10",
    "SIMULATED_HOUR_SECONDS": "60",
    "NUM_PERIODIC_GROUPS": "48",
}


def project_root() -> Path:
    """Return the repository root based on this module's location."""
    return Path(__file__).resolve().parents[1]


def load_env_file(path: Path) -> dict[str, str]:
    """Read KEY=VALUE settings, preferring already-set process variables."""

    values = dict(os.environ)
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return values


def _setting(values: dict[str, str], key: str) -> str:
    """Return an override or the project's safe default for ``key``."""

    value = values.get(key)
    if value is None:
        return DEFAULT_ENV[key]
    return value.strip()


def _positive_int(values: dict[str, str], key: str, *, maximum: int | None = None) -> int:
    raw = _setting(values, key)
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"{key} must be an integer, received {raw!r}.") from error
    if value <= 0 or (maximum is not None and value > maximum):
        suffix = f" and at most {maximum}" if maximum is not None else ""
        raise ValueError(f"{key} must be positive{suffix}, received {value}.")
    return value


def load_runtime_config(env_path: Path | None = None) -> RuntimeConfig:
    """Load validated settings from process variables and an optional ``.env``.

    Process variables take precedence over the file.  A password may be blank
    for PostgreSQL peer/trust authentication; no password is ever invented.
    """

    root = project_root()
    env_path = env_path or root / ".env"
    values = load_env_file(env_path)
    online_file = Path(_setting(values, "ONLINE_SALES_FILE"))
    if not online_file.is_absolute():
        online_file = root / online_file

    port = _positive_int(values, "PGPORT", maximum=65_535)
    master_partition_size = _positive_int(values, "MD_PARTITION_SIZE")
    stream_chunk_size = _positive_int(values, "STREAM_CHUNK_SIZE")
    stream_buffer_limit = _positive_int(values, "STREAM_BUFFER_LIMIT")
    if stream_buffer_limit < stream_chunk_size:
        raise ValueError("STREAM_BUFFER_LIMIT must be greater than or equal to STREAM_CHUNK_SIZE.")

    return RuntimeConfig(
        database=DatabaseConfig(
            host=_setting(values, "PGHOST"),
            port=port,
            database=_setting(values, "PGDATABASE"),
            user=_setting(values, "PGUSER"),
            password=_setting(values, "PGPASSWORD"),
        ),
        online_file=online_file,
        master_partition_size=master_partition_size,
        stream_chunk_size=stream_chunk_size,
        stream_buffer_limit=stream_buffer_limit,
        peak_rate=_positive_int(values, "PEAK_RATE"),
        non_peak_rate=_positive_int(values, "NON_PEAK_RATE"),
        simulated_hour_seconds=_positive_int(values, "SIMULATED_HOUR_SECONDS"),
        periodic_group_count=_positive_int(values, "NUM_PERIODIC_GROUPS", maximum=32_767),
    )


def connect_database(config: DatabaseConfig):
    """Open a PostgreSQL connection using an installed supported driver."""
    parameters = {
        "host": config.host,
        "port": config.port,
        "dbname": config.database,
        "user": config.user,
        "connect_timeout": 5,
        "application_name": "dwbi_project",
    }
    if config.password:
        parameters["password"] = config.password
    if psycopg is not None:
        return psycopg.connect(**parameters)
    if psycopg2 is not None:
        return psycopg2.connect(**parameters)
    raise RuntimeError("Install psycopg (recommended) or psycopg2 before running the project.")
