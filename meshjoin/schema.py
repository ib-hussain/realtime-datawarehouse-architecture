"""PostgreSQL schema inspection and explicit project-schema installation."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from meshjoin.base import DatabaseConfig, connect_database

PREPARATION_RELATIONS = (
    "oltp_online.online_sales_stream",
    "oltp_physical.periodic_sales_data",
    "staging_area.customer_master",
    "staging_area.product_master",
    "staging_area.load_manifest",
)

RUNTIME_RELATIONS = (
    *PREPARATION_RELATIONS,
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


def relation_status(connection, relations: Iterable[str] = RUNTIME_RELATIONS) -> dict[str, bool]:
    """Return whether every fully-qualified relation resolves in PostgreSQL."""

    result: dict[str, bool] = {}
    with connection.cursor() as cursor:
        for relation in relations:
            cursor.execute("SELECT to_regclass(%s)", (relation,))
            result[relation] = cursor.fetchone()[0] is not None
    return result


def inspect_schema(
    config: DatabaseConfig,
    relations: Iterable[str] = RUNTIME_RELATIONS,
) -> dict[str, bool]:
    connection = connect_database(config)
    try:
        return relation_status(connection, relations)
    finally:
        connection.close()


def missing_relations(
    config: DatabaseConfig,
    relations: Iterable[str] = RUNTIME_RELATIONS,
) -> list[str]:
    return [name for name, exists in inspect_schema(config, relations).items() if not exists]


def require_relations(
    config: DatabaseConfig,
    relations: Iterable[str] = RUNTIME_RELATIONS,
) -> None:
    missing = missing_relations(config, relations)
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(
            "The PostgreSQL project schema is incomplete. Missing: "
            f"{names}. Run `python cli_app.py init-db --yes` on the intended "
            "disposable project database, then run `python cli_app.py prepare "
            "--load-db`."
        )


def apply_schema(config: DatabaseConfig, schema_path: Path) -> int:
    """Execute the destructive project schema file and return statement count.

    Callers are responsible for obtaining an explicit confirmation before this
    function is invoked.  Statements are executed one by one so the behavior is
    consistent across psycopg 3 and psycopg2.
    """

    if not schema_path.is_file():
        raise FileNotFoundError(schema_path)

    try:
        import sqlparse
    except ImportError as error:  # pragma: no cover - dependency check path.
        raise RuntimeError("Install project requirements before applying the schema.") from error

    statements = [
        statement.strip()
        for statement in sqlparse.split(schema_path.read_text(encoding="utf-8"))
        if statement.strip()
    ]
    connection = connect_database(config)
    try:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(statements)
