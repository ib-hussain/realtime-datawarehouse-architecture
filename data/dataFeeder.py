"""Version 1 parallel feeder for the MESHJOIN stream buffer.

Two PostgreSQL reads run concurrently, each on its own connection:

* ``oltp_online.online_sales_stream``
* ``oltp_physical.periodic_sales_data``

The dispatcher places both sources into one bounded stream buffer. When rows
from both sources are ready, online rows are selected first. The feeder does
not mark periodic rows as released or insert into the warehouse; MESHJOIN owns
those state changes after a row has completed processing.

Typical integration pattern::

    buffer = StreamBuffer(maxsize=1000)
    counts = parallel_feed(db_config, buffer)

The MESHJOIN consumer should call ``buffer.get()`` while ``parallel_feed`` is
running, otherwise the bounded buffer will intentionally apply back-pressure.
"""

from __future__ import annotations

import argparse
import queue
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from getpass import getpass
from pathlib import Path
from typing import Any
from uuid import uuid4

try:  # psycopg 3 is preferred.
    import psycopg  # type: ignore
except ImportError:  # pragma: no cover - depends on the user's environment.
    psycopg = None

try:  # psycopg2 is supported as a fallback.
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover - depends on the user's environment.
    psycopg2 = None


DEFAULT_BUFFER_SIZE = 1_000
DEFAULT_BATCH_SIZE = 500
_DONE = object()


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class SaleRecord:
    """A normalized row passed from a source schema to MESHJOIN."""

    source: str
    source_row_id: int
    transaction_id: str
    event_time: datetime
    customer_id: int
    product_id: str
    quantity: int
    unit_price: Decimal
    sales_mode: bool
    release_hour: int | None = None


class StreamBuffer:
    """Thread-safe bounded buffer used between feeder and MESHJOIN."""

    def __init__(self, maxsize: int = DEFAULT_BUFFER_SIZE) -> None:
        if maxsize <= 0:
            raise ValueError("Stream buffer maxsize must be positive.")
        self._queue: queue.Queue[SaleRecord] = queue.Queue(maxsize=maxsize)

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    def put(
        self,
        record: SaleRecord,
        stop_event: threading.Event | None = None,
    ) -> None:
        """Put a row, waiting briefly so a stop request can be observed."""

        while True:
            if stop_event is not None and stop_event.is_set():
                raise FeedStopped("Feeder stopped while waiting for stream space.")
            try:
                self._queue.put(record, timeout=0.2)
                return
            except queue.Full:
                continue

    def get(self, timeout: float | None = None) -> SaleRecord:
        return self._queue.get(timeout=timeout)

    def task_done(self) -> None:
        self._queue.task_done()

    def qsize(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()


class FeedStopped(RuntimeError):
    """Raised when the caller requests feeder shutdown."""


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
        "Install psycopg (recommended) or psycopg2 before reading PostgreSQL."
    )


def _as_record(source: str, row: tuple[Any, ...]) -> SaleRecord:
    if source == "online":
        row_id, transaction_id, event_time, customer_id, product_id, quantity, unit_price, sales_mode = row
        return SaleRecord(
            source="online",
            source_row_id=int(row_id),
            transaction_id=str(transaction_id),
            event_time=event_time,
            customer_id=int(customer_id),
            product_id=str(product_id),
            quantity=int(quantity),
            unit_price=Decimal(unit_price),
            sales_mode=bool(sales_mode),
        )

    (
        row_id,
        transaction_id,
        event_time,
        customer_id,
        product_id,
        quantity,
        unit_price,
        sales_mode,
        release_hour,
    ) = row
    return SaleRecord(
        source="periodic",
        source_row_id=int(row_id),
        transaction_id=str(transaction_id),
        event_time=event_time,
        customer_id=int(customer_id),
        product_id=str(product_id),
        quantity=int(quantity),
        unit_price=Decimal(unit_price),
        sales_mode=bool(sales_mode),
        release_hour=int(release_hour),
    )


def _read_source(
    config: DatabaseConfig,
    source: str,
    output_queue: queue.Queue[object],
    batch_size: int,
    stop_event: threading.Event,
    periodic_release_hour: int | None,
) -> int:
    """Read one source using an independent PostgreSQL connection."""

    if source == "online":
        query = """
            SELECT stream_row_id, transaction_id, event_time, customer_id,
                   product_id, quantity, unit_price, sales_mode
            FROM oltp_online.online_sales_stream
            ORDER BY stream_row_id
        """
        parameters: tuple[Any, ...] = ()
    else:
        query = """
            SELECT periodic_row_id, transaction_id, event_time, customer_id,
                   product_id, quantity, unit_price, sales_mode, release_hour
            FROM oltp_physical.periodic_sales_data
            WHERE released = FALSE
        """
        parameters = ()
        if periodic_release_hour is not None:
            query += " AND release_hour = %s"
            parameters = (periodic_release_hour,)
        query += " ORDER BY release_hour, event_time, periodic_row_id"

    cursor_name = f"feed_{source}_{uuid4().hex}"
    count = 0
    connection = None
    try:
        # Connection creation is inside the protected section as well, so a
        # connection error still emits the worker sentinel and cannot leave
        # the dispatcher waiting forever.
        connection = connect_database(config)
        # Named cursors keep the complete source table out of client memory.
        with connection.cursor(name=cursor_name) as cursor:
            if hasattr(cursor, "itersize"):
                cursor.itersize = batch_size
            cursor.execute(query, parameters)
            while not stop_event.is_set():
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    while not stop_event.is_set():
                        try:
                            output_queue.put(
                                _as_record(source, row),
                                timeout=0.2,
                            )
                            count += 1
                            break
                        except queue.Full:
                            continue
    finally:
        if connection is not None:
            connection.close()
        # The sentinel is placed after all rows. If shutdown is requested,
        # still try briefly so the dispatcher can terminate cleanly.
        while True:
            try:
                output_queue.put(_DONE, timeout=0.2)
                break
            except queue.Full:
                if stop_event.is_set():
                    break
    return count


def _take_next(
    online_queue: queue.Queue[object],
    periodic_queue: queue.Queue[object],
    online_done: bool,
    periodic_done: bool,
) -> tuple[str | None, object | None, bool, bool]:
    """Select online first whenever a row is currently available."""

    if not online_done:
        try:
            item = online_queue.get_nowait()
            if item is _DONE:
                online_done = True
            else:
                return "online", item, online_done, periodic_done
        except queue.Empty:
            pass

    if not periodic_done:
        try:
            item = periodic_queue.get_nowait()
            if item is _DONE:
                periodic_done = True
            else:
                return "periodic", item, online_done, periodic_done
        except queue.Empty:
            pass

    return None, None, online_done, periodic_done


def parallel_feed(
    config: DatabaseConfig,
    stream_buffer: StreamBuffer,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    periodic_release_hour: int | None = None,
    stop_event: threading.Event | None = None,
) -> dict[str, int]:
    """Read both PostgreSQL source schemas in parallel and fill the buffer.

    ``periodic_release_hour`` can be supplied by the future PeriodicThread to
    feed only one of the 48 release groups. ``None`` reads all unreleased
    periodic rows, which is useful for v1 integration tests.
    """

    if batch_size <= 0:
        raise ValueError("batch_size must be positive.")
    local_stop = stop_event or threading.Event()
    source_queue_size = max(stream_buffer.maxsize, batch_size * 2)
    online_queue: queue.Queue[object] = queue.Queue(maxsize=source_queue_size)
    periodic_queue: queue.Queue[object] = queue.Queue(maxsize=source_queue_size)

    with ThreadPoolExecutor(
        max_workers=2,
        thread_name_prefix="postgres-feed",
    ) as executor:
        futures: dict[str, Future[int]] = {
            "online": executor.submit(
                _read_source,
                config,
                "online",
                online_queue,
                batch_size,
                local_stop,
                periodic_release_hour,
            ),
            "periodic": executor.submit(
                _read_source,
                config,
                "periodic",
                periodic_queue,
                batch_size,
                local_stop,
                periodic_release_hour,
            ),
        }

        online_done = False
        periodic_done = False
        dispatched = {"online": 0, "periodic": 0}

        while not local_stop.is_set() and not (online_done and periodic_done):
            source, item, online_done, periodic_done = _take_next(
                online_queue,
                periodic_queue,
                online_done,
                periodic_done,
            )
            if source is None:
                time.sleep(0.005)
                continue
            stream_buffer.put(item, local_stop)  # type: ignore[arg-type]
            dispatched[source] += 1

        # Surface a worker-side database error to the caller.
        worker_counts = {source: future.result() for source, future in futures.items()}

    if local_stop.is_set() and not (online_done and periodic_done):
        raise FeedStopped("Parallel feed stopped before both sources completed.")

    # Worker and dispatcher counts should agree unless shutdown interrupted the
    # run. Returning both makes v1 diagnostics straightforward.
    return {
        "online_read": worker_counts["online"],
        "periodic_read": worker_counts["periodic"],
        "online_dispatched": dispatched["online"],
        "periodic_dispatched": dispatched["periodic"],
    }


def prompt_or(value: str | None, message: str, default: str | None = None) -> str:
    if value is not None:
        return value
    suffix = f" [{default}]" if default is not None else ""
    answer = input(f"{message}{suffix}: ").strip()
    return answer or (default or "")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--database")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--buffer-size", type=int, default=DEFAULT_BUFFER_SIZE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--periodic-release-hour", type=int, choices=range(48))
    parser.add_argument(
        "--demo-drain",
        action="store_true",
        help="Drain rows in a demo consumer so this module can be run standalone.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = DatabaseConfig(
        host=prompt_or(args.host, "PostgreSQL host", "localhost"),
        port=int(prompt_or(args.port, "PostgreSQL port", "5432")),
        database=prompt_or(args.database, "Database name"),
        user=prompt_or(args.user, "Database user"),
        password=args.password if args.password is not None else getpass("Database password: "),
    )
    buffer = StreamBuffer(args.buffer_size)
    stop_event = threading.Event()

    if not args.demo_drain:
        raise SystemExit(
            "v1 is a producer for MESHJOIN. Import parallel_feed() from your "
            "consumer, or rerun with --demo-drain for a standalone test."
        )

    def drain() -> None:
        while not stop_event.is_set():
            try:
                buffer.get(timeout=0.2)
                buffer.task_done()
            except queue.Empty:
                continue

    consumer = threading.Thread(target=drain, name="demo-stream-consumer", daemon=True)
    consumer.start()
    try:
        counts = parallel_feed(
            config,
            buffer,
            batch_size=args.batch_size,
            periodic_release_hour=args.periodic_release_hour,
            stop_event=stop_event,
        )
        print(counts)
    finally:
        stop_event.set()


if __name__ == "__main__":
    main()

