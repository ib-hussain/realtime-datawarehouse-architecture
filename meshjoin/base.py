"""Shared configuration and PostgreSQL connection helpers."""

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
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class RuntimeConfig:
    database: DatabaseConfig
    online_file: Path
    master_partition_size: int = 500
    stream_chunk_size: int = 500
    stream_buffer_limit: int = 1_000
    peak_rate: int = 80
    non_peak_rate: int = 10
    simulated_hour_seconds: int = 60
    periodic_group_count: int = 48


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_env_file(path: Path) -> dict[str, str]:
    """Read the small KEY=VALUE .env format used by this project."""

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


def require(values: dict[str, str], key: str) -> str:
    value = values.get(key, "").strip()
    if not value:
        raise ValueError(f"Missing {key} in .env or environment.")
    return value


def load_runtime_config(env_path: Path | None = None) -> RuntimeConfig:
    root = project_root()
    values = load_env_file(env_path or root / ".env")
    online_file = Path(require(values, "ONLINE_SALES_FILE"))
    if not online_file.is_absolute():
        online_file = root / online_file

    return RuntimeConfig(
        database=DatabaseConfig(
            host=require(values, "PGHOST"),
            port=int(require(values, "PGPORT")),
            database=require(values, "PGDATABASE"),
            user=require(values, "PGUSER"),
            password=require(values, "PGPASSWORD"),
        ),
        online_file=online_file,
        master_partition_size=int(values.get("MD_PARTITION_SIZE", "500")),
        stream_chunk_size=int(values.get("STREAM_CHUNK_SIZE", "500")),
        stream_buffer_limit=int(values.get("STREAM_BUFFER_LIMIT", "1000")),
        peak_rate=int(values.get("PEAK_RATE", "80")),
        non_peak_rate=int(values.get("NON_PEAK_RATE", "10")),
        simulated_hour_seconds=int(values.get("SIMULATED_HOUR_SECONDS", "60")),
        periodic_group_count=int(values.get("NUM_PERIODIC_GROUPS", "48")),
    )


def connect_database(config: DatabaseConfig):
    if psycopg is not None:
        return psycopg.connect(
            host=config.host,
            port=config.port,
            dbname=config.database,
            user=config.user,
            password=config.password,
        )
    if psycopg2 is not None:
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.database,
            user=config.user,
            password=config.password,
        )
    raise RuntimeError(
        "Install psycopg (recommended) or psycopg2 before running the project."
    )

