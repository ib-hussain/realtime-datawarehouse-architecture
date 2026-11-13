"""Online, periodic, and shed-data producer threads for the MESHJOIN buffer."""

from __future__ import annotations

import csv
import hashlib
import threading
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from meshjoin.base import DatabaseConfig, connect_database
from meshjoin.stream_buffer import PriorityStreamBuffer, Transaction


DATE_FORMAT = "%d/%m/%Y %H:%M"


class RuntimeState:
    """Thread-safe command state shared by the three project threads."""

    VALID_SOURCES = {"online", "periodic", "both"}
    VALID_RATES = {"peak", "non-peak"}

    def __init__(self, arrival_mode: str = "peak") -> None:
        self._mode = "both"
        self._arrival_mode = arrival_mode
        self._condition = threading.Condition()
        self.stop_event = threading.Event()

    def set_source_mode(self, mode: str) -> None:
        if mode not in self.VALID_SOURCES:
            raise ValueError(f"Unknown source mode: {mode}")
        with self._condition:
            self._mode = mode
            self._condition.notify_all()

    def set_arrival_mode(self, mode: str) -> None:
        if mode not in self.VALID_RATES:
            raise ValueError(f"Unknown arrival mode: {mode}")
        with self._condition:
            self._arrival_mode = mode
            self._condition.notify_all()

    def allows(self, source: str) -> bool:
        with self._condition:
            return self._mode in (source, "both")

    def is_peak(self) -> bool:
        with self._condition:
            return self._arrival_mode == "peak"

    def snapshot(self) -> tuple[str, str]:
        with self._condition:
            return self._mode, self._arrival_mode

    def wait_until_allowed(self, source: str) -> bool:
        with self._condition:
            while not self.stop_event.is_set() and self._mode not in (source, "both"):
                self._condition.wait(timeout=0.25)
            return not self.stop_event.is_set()

    def stop(self) -> None:
        self.stop_event.set()
        with self._condition:
            self._condition.notify_all()


def store_id_for(transaction_id: str, sales_mode: bool) -> int:
    """Apply the project's deterministic store rule."""

    if sales_mode:
        return 1
    digest_prefix = hashlib.md5(transaction_id.encode("utf-8")).hexdigest()[:8]
    return 2 + (int(digest_prefix, 16) % 4)


class ShedRepository:
    """Persistent disk queue for periodic rows shed during peak load."""

    def __init__(self, database: DatabaseConfig) -> None:
        self._connection = connect_database(database)

    def shed(self, transaction: Transaction) -> None:
        if transaction.source != "periodic" or transaction.source_row_id is None:
            raise ValueError("Only a periodic row with periodic_row_id may be shed.")
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO waiting_area.shed_table_physical
                    (periodic_row_id, transaction_id, event_time, customer_id,
                     product_id, quantity, unit_price, sales_mode, release_hour)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (periodic_row_id) DO NOTHING
                """,
                (
                    transaction.source_row_id,
                    transaction.transaction_id,
                    transaction.event_time,
                    transaction.customer_id,
                    transaction.product_id,
                    transaction.quantity,
                    transaction.unit_price,
                    transaction.sales_mode,
                    transaction.release_hour,
                ),
            )
        self._connection.commit()

    def load_when_idle(self, buffer: PriorityStreamBuffer, limit: int = 500) -> int:
        """Move shed rows back into memory and mark only inserted rows done."""

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT shed_id, periodic_row_id, transaction_id, event_time,
                       customer_id, product_id, quantity, unit_price,
                       sales_mode, release_hour
                FROM waiting_area.shed_table_physical
                WHERE processed = FALSE
                ORDER BY shed_at, shed_id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (limit,),
            )
            rows = cursor.fetchall()

            completed_ids: list[int] = []
            for row in rows:
                (
                    shed_id,
                    periodic_row_id,
                    transaction_id,
                    event_time,
                    customer_id,
                    product_id,
                    quantity,
                    unit_price,
                    sales_mode,
                    release_hour,
                ) = row
                transaction = Transaction(
                    source="shed",
                    source_row_id=int(periodic_row_id),
                    transaction_id=str(transaction_id),
                    event_time=event_time,
                    customer_id=int(customer_id),
                    product_id=str(product_id),
                    quantity=int(quantity),
                    unit_price=Decimal(unit_price),
                    sales_mode=bool(sales_mode),
                    release_hour=int(release_hour),
                    processing_mode="Shed",
                )
                if not buffer.try_put(transaction):
                    break
                completed_ids.append(int(shed_id))

            if completed_ids:
                cursor.execute(
                    """
                    UPDATE waiting_area.shed_table_physical
                    SET processed = TRUE, processed_at = CURRENT_TIMESTAMP
                    WHERE shed_id = ANY(%s)
                    """,
                    (completed_ids,),
                )
        self._connection.commit()
        return len(completed_ids)

    def pending_count(self) -> int:
        with self._connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM waiting_area.shed_table_physical WHERE processed = FALSE"
            )
            return int(cursor.fetchone()[0])

    def close(self) -> None:
        self._connection.close()


class OnlineThread(threading.Thread):
    """Reads online.csv as a rate-controlled stream."""

    def __init__(
        self,
        online_file: Path,
        buffer: PriorityStreamBuffer,
        state: RuntimeState,
        peak_rate: int,
        non_peak_rate: int,
    ) -> None:
        super().__init__(name="OnlineThread", daemon=True)
        self.online_file = online_file
        self.buffer = buffer
        self.state = state
        self.peak_rate = peak_rate
        self.non_peak_rate = non_peak_rate
        self.rows_loaded = 0
        self.error: Exception | None = None

    def run(self) -> None:
        try:
            with self.online_file.open("r", encoding="utf-8-sig", newline="") as source:
                for row in csv.DictReader(source):
                    if not self.state.wait_until_allowed("online"):
                        return
                    transaction = Transaction(
                        source="online",
                        source_row_id=None,
                        transaction_id=row["transaction_id"].strip(),
                        event_time=datetime.strptime(row["event_time"].strip(), DATE_FORMAT),
                        customer_id=int(Decimal(row["customer_id"])),
                        product_id=row["product_id"].strip(),
                        quantity=int(row["quantity"]),
                        unit_price=Decimal(row["unit_price"]),
                        sales_mode=True,
                    )
                    if not self.buffer.put(transaction, self.state.stop_event):
                        return
                    self.rows_loaded += 1
                    rate = self.peak_rate if self.state.is_peak() else self.non_peak_rate
                    self.state.stop_event.wait(1.0 / rate)
        except Exception as error:  # surfaced by the coordinator status loop
            self.error = error
            self.state.stop()


class PeriodicThread(threading.Thread):
    """Extracts one periodic release group every simulated hour."""

    def __init__(
        self,
        database: DatabaseConfig,
        buffer: PriorityStreamBuffer,
        state: RuntimeState,
        simulated_hour_seconds: int,
        group_count: int,
    ) -> None:
        super().__init__(name="PeriodicThread", daemon=True)
        self.database = database
        self.buffer = buffer
        self.state = state
        self.simulated_hour_seconds = simulated_hour_seconds
        self.group_count = group_count
        self.rows_loaded = 0
        self.rows_shed = 0
        self.groups_released = 0
        self.error: Exception | None = None

    def run(self) -> None:
        connection = connect_database(self.database)
        shed = ShedRepository(self.database)
        try:
            for group in range(self.group_count):
                if not self.state.wait_until_allowed("periodic"):
                    return
                loaded, shed_count = self._extract_group(connection, shed, group)
                self.rows_loaded += loaded
                self.rows_shed += shed_count
                self.groups_released += 1
                if self.state.stop_event.wait(self.simulated_hour_seconds):
                    return
        except Exception as error:
            self.error = error
            self.state.stop()
        finally:
            shed.close()
            connection.close()

    def _extract_group(self, connection, shed: ShedRepository, group: int) -> tuple[int, int]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT periodic_row_id, transaction_id, event_time, customer_id,
                       product_id, quantity, unit_price, sales_mode, release_hour
                FROM oltp_physical.periodic_sales_data
                WHERE released = FALSE AND release_hour = %s
                ORDER BY event_time, periodic_row_id
                """,
                (group,),
            )
            rows = cursor.fetchall()

        loaded = 0
        shed_count = 0
        for row in rows:
            (
                periodic_row_id,
                transaction_id,
                event_time,
                customer_id,
                product_id,
                quantity,
                unit_price,
                sales_mode,
                release_hour,
            ) = row
            transaction = Transaction(
                source="periodic",
                source_row_id=int(periodic_row_id),
                transaction_id=str(transaction_id),
                event_time=event_time,
                customer_id=int(customer_id),
                product_id=str(product_id),
                quantity=int(quantity),
                unit_price=Decimal(unit_price),
                sales_mode=bool(sales_mode),
                release_hour=int(release_hour),
            )
            if self.state.is_peak() and not self.buffer.try_put(transaction):
                shed.shed(transaction)
                shed_count += 1
            elif self.buffer.put(transaction, self.state.stop_event):
                loaded += 1
            else:
                return loaded, shed_count

        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE oltp_physical.periodic_sales_data
                SET released = TRUE
                WHERE released = FALSE AND release_hour = %s
                """,
                (group,),
            )
        connection.commit()
        return loaded, shed_count

