"""MESHJOIN passes and final warehouse loading."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from datetime import datetime

from meshjoin.base import DatabaseConfig, RuntimeConfig, connect_database
from meshjoin.master_data import MasterDataPager
from meshjoin.stream_buffer import PriorityStreamBuffer, Transaction
from meshjoin.streamer import RuntimeState, ShedRepository, store_id_for


@dataclass
class Chunk:
    """Queue entry that points to, but does not copy, hash-table transactions."""

    transactions: list[Transaction]
    pages_seen: int = 0


class MeshJoinPass:
    """One cyclic MESHJOIN pass over a disk-resident master-data relation."""

    def __init__(
        self,
        pager: MasterDataPager,
        join_key: Callable[[Transaction], Hashable],
        master_key: Callable[[tuple], Hashable],
        enrich: Callable[[Transaction, tuple], None],
        is_complete: Callable[[Transaction], bool],
    ) -> None:
        self.pager = pager
        self.join_key = join_key
        self.master_key = master_key
        self.enrich = enrich
        self.is_complete = is_complete
        self._hash_table: dict[Hashable, list[Transaction]] = {}
        self._queue: deque[Chunk] = deque()
        self.unmatched = 0

    @property
    def active_transactions(self) -> int:
        return sum(len(chunk.transactions) for chunk in self._queue)

    @property
    def has_pending(self) -> bool:
        return bool(self._queue)

    def step(self, incoming: list[Transaction]) -> list[Transaction]:
        """Perform one PDF-defined MESHJOIN iteration.

        1. Load the next master-data partition from disk.
        2. Put up to one incoming chunk into H and the FIFO queue.
        3. Probe H with the disk page and enrich matches.
        4. Release only chunks that have seen every master-data partition.
        """

        if incoming:
            chunk = Chunk(incoming)
            self._queue.append(chunk)
            for transaction in incoming:
                self._hash_table.setdefault(self.join_key(transaction), []).append(transaction)

        page = self.pager.next_page()
        for master_row in page.rows:
            for transaction in self._hash_table.get(self.master_key(master_row), ()):
                self.enrich(transaction, master_row)

        for chunk in self._queue:
            chunk.pages_seen += 1

        completed: list[Transaction] = []
        while self._queue and self._queue[0].pages_seen >= page.partition_count:
            chunk = self._queue.popleft()
            for transaction in chunk.transactions:
                self._remove_from_hash(transaction)
                if self.is_complete(transaction):
                    completed.append(transaction)
                else:
                    self.unmatched += 1
        return completed

    def _remove_from_hash(self, transaction: Transaction) -> None:
        key = self.join_key(transaction)
        values = self._hash_table[key]
        values.remove(transaction)
        if not values:
            del self._hash_table[key]


class WarehouseLoader:
    """Loads dimensions, the product-store bridge, and fact transactions."""

    def __init__(
        self,
        database: DatabaseConfig,
        stores: dict[int, tuple[str, str]],
        flush_size: int = 500,
        bonus_cache: bool = True,
    ) -> None:
        self._connection = connect_database(database)
        self._stores = stores
        self.flush_size = flush_size
        self.bonus_cache = bonus_cache
        self._known_customers: set[int] = set()
        self._known_products: set[str] = set()
        self._known_suppliers: set[str] = set()
        self._known_stores: set[int] = set()
        self._time_keys: dict[datetime, int] = {}
        self._product_store_keys: dict[tuple[str, int], int] = {}
        self._facts: list[tuple] = []
        self._online_cache: list[tuple] = []
        self._periodic_source_ids: list[int] = []
        self._shed_source_ids: list[int] = []
        self.cached_count = 0
        self.loaded_count = 0

    def stage(self, transaction: Transaction) -> None:
        self._ensure_dimensions(transaction)
        store_id = store_id_for(transaction.transaction_id, transaction.sales_mode)
        time_key = self._time_keys[transaction.event_time]
        product_store_key = self._product_store_keys[(transaction.product_id, store_id)]
        source_type = "online" if transaction.source == "online" else "periodic"
        target = (
            self._online_cache if (self.bonus_cache and transaction.sales_mode) else self._facts
        )
        target.append(
            (
                transaction.transaction_id,
                source_type,
                transaction.source_row_id,
                transaction.customer_id,
                transaction.product_id,
                transaction.supplier_id,
                store_id,
                time_key,
                product_store_key,
                transaction.event_time,
                transaction.quantity,
                transaction.unit_price,
                transaction.revenue,
                transaction.sales_mode,
                transaction.processing_mode,
            )
        )
        if source_type == "periodic":
            self._periodic_source_ids.append(transaction.source_row_id)
        if transaction.source == "shed":
            self._shed_source_ids.append(transaction.source_row_id)
        if len(self._facts) + len(self._online_cache) >= self.flush_size:
            self.flush()

    def flush(self) -> None:
        if not self._facts and not self._online_cache:
            return
        columns = "(transaction_id, source_type, source_row_id, customer_id, product_id, supplier_id, store_id, time_key, product_store_key, event_time, quantity, unit_price, revenue, sales_mode, processing_mode)"
        values = "(" + ", ".join(["%s"] * 15) + ")"
        facts_inserted = 0
        cache_inserted = 0
        try:
            with self._connection.cursor() as cursor:
                if self._facts:
                    cursor.executemany(
                        "INSERT INTO data_warehouse.transactions "
                        + columns
                        + " VALUES "
                        + values
                        + " ON CONFLICT (source_type, source_row_id) DO NOTHING",
                        self._facts,
                    )
                    facts_inserted = max(0, cursor.rowcount)
                if self._online_cache:
                    cursor.executemany(
                        "INSERT INTO data_warehouse.live_cache "
                        + columns
                        + " SELECT incoming.* FROM (VALUES "
                        + values
                        + ") AS incoming(transaction_id, source_type, source_row_id, "
                        "customer_id, product_id, supplier_id, store_id, time_key, "
                        "product_store_key, event_time, quantity, unit_price, revenue, "
                        "sales_mode, processing_mode) WHERE NOT EXISTS (SELECT 1 FROM "
                        "data_warehouse.transactions fact WHERE fact.source_type = "
                        "incoming.source_type AND fact.source_row_id = incoming.source_row_id)"
                        + " ON CONFLICT (source_type, source_row_id) DO NOTHING",
                        self._online_cache,
                    )
                    cache_inserted = max(0, cursor.rowcount)
                if self._periodic_source_ids:
                    cursor.execute(
                        """
                        UPDATE oltp_physical.periodic_sales_data
                        SET released = TRUE
                        WHERE periodic_row_id = ANY(%s)
                        """,
                        (self._periodic_source_ids,),
                    )
                if self._shed_source_ids:
                    cursor.execute(
                        """
                        UPDATE waiting_area.shed_table_physical
                        SET processed = TRUE,
                            in_progress = FALSE,
                            claimed_at = NULL,
                            processed_at = CURRENT_TIMESTAMP
                        WHERE periodic_row_id = ANY(%s)
                        """,
                        (self._shed_source_ids,),
                    )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        self.loaded_count += facts_inserted
        self.cached_count += cache_inserted
        self._facts.clear()
        self._online_cache.clear()
        self._periodic_source_ids.clear()
        self._shed_source_ids.clear()

    def drain_cache(self, limit: int = 500) -> int:
        """Atomically move cached online rows to fact during non-peak operation."""
        self.flush()
        fields = "transaction_id, source_type, source_row_id, customer_id, product_id, supplier_id, store_id, time_key, product_store_key, event_time, quantity, unit_price, revenue, sales_mode, processing_mode"
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    WITH selected AS (
                        SELECT cache_id FROM data_warehouse.live_cache
                        ORDER BY cache_id LIMIT %s FOR UPDATE SKIP LOCKED
                    ), moved AS (
                        DELETE FROM data_warehouse.live_cache c
                        USING selected s WHERE c.cache_id = s.cache_id
                        RETURNING c.{", c.".join(fields.split(", "))}
                    )
                    INSERT INTO data_warehouse.transactions ({fields})
                    SELECT {fields} FROM moved
                    ON CONFLICT (source_type, source_row_id) DO NOTHING
                """,
                    (limit,),
                )
                count = cursor.rowcount
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        self.loaded_count += count
        return count

    def close(self) -> None:
        self.flush()
        self._connection.close()

    def _ensure_dimensions(self, transaction: Transaction) -> None:
        if not transaction.customer_complete or not transaction.product_complete:
            raise ValueError("Cannot load a transaction before both MESHJOIN passes complete.")
        store_id = store_id_for(transaction.transaction_id, transaction.sales_mode)

        with self._connection.cursor() as cursor:
            if transaction.customer_id not in self._known_customers:
                cursor.execute(
                    """
                    INSERT INTO data_warehouse.dim_customer
                        (customer_id, gender, age_group, occupation, city_category,
                         stay_in_current_city_years, marital_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO UPDATE SET
                        gender = EXCLUDED.gender,
                        age_group = EXCLUDED.age_group,
                        occupation = EXCLUDED.occupation,
                        city_category = EXCLUDED.city_category,
                        stay_in_current_city_years = EXCLUDED.stay_in_current_city_years,
                        marital_status = EXCLUDED.marital_status
                    """,
                    (
                        transaction.customer_id,
                        transaction.gender,
                        transaction.age_group,
                        transaction.occupation,
                        transaction.city_category,
                        transaction.stay_in_current_city_years,
                        transaction.marital_status,
                    ),
                )
                self._known_customers.add(transaction.customer_id)

            if transaction.product_id not in self._known_products:
                cursor.execute(
                    """
                    INSERT INTO data_warehouse.dim_product
                        (product_id, product_name, product_category)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (product_id) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        product_category = EXCLUDED.product_category
                    """,
                    (
                        transaction.product_id,
                        transaction.product_name,
                        transaction.product_category,
                    ),
                )
                self._known_products.add(transaction.product_id)

            if transaction.supplier_id not in self._known_suppliers:
                cursor.execute(
                    """
                    INSERT INTO data_warehouse.dim_supplier (supplier_id, supplier_name)
                    VALUES (%s, %s)
                    ON CONFLICT (supplier_id) DO UPDATE SET
                        supplier_name = EXCLUDED.supplier_name
                    """,
                    (transaction.supplier_id, transaction.supplier_name),
                )
                self._known_suppliers.add(str(transaction.supplier_id))

            if store_id not in self._known_stores:
                store_name, city = self._stores[store_id]
                cursor.execute(
                    """
                    INSERT INTO data_warehouse.dim_store (store_id, store_name, city)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (store_id) DO UPDATE SET
                        store_name = EXCLUDED.store_name,
                        city = EXCLUDED.city
                    """,
                    (store_id, store_name, city),
                )
                self._known_stores.add(store_id)

            if transaction.event_time not in self._time_keys:
                cursor.execute(
                    """
                    INSERT INTO data_warehouse.dim_time
                        (event_time, date, hour, day_of_month, month, quarter,
                         year, day_of_week, is_weekend, season, half_year)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_time) DO UPDATE SET event_time = EXCLUDED.event_time
                    RETURNING time_key
                    """,
                    self._time_values(transaction.event_time),
                )
                self._time_keys[transaction.event_time] = int(cursor.fetchone()[0])

            product_store = (transaction.product_id, store_id)
            if product_store not in self._product_store_keys:
                cursor.execute(
                    """
                    INSERT INTO data_warehouse.products_store (product_id, store_id)
                    VALUES (%s, %s)
                    ON CONFLICT (product_id, store_id) DO UPDATE SET
                        product_id = EXCLUDED.product_id
                    RETURNING product_store_key
                    """,
                    product_store,
                )
                self._product_store_keys[product_store] = int(cursor.fetchone()[0])

    @staticmethod
    def _time_values(event_time: datetime) -> tuple:
        weekdays = (
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        )
        month = event_time.month
        season = (
            "Spring"
            if month in (3, 4, 5)
            else "Summer"
            if month in (6, 7, 8)
            else "Fall"
            if month in (9, 10, 11)
            else "Winter"
        )
        return (
            event_time,
            event_time.date(),
            event_time.hour,
            event_time.day,
            month,
            (month - 1) // 3 + 1,
            event_time.year,
            weekdays[event_time.weekday()],
            event_time.weekday() >= 5,
            season,
            "First" if month <= 6 else "Second",
        )


class MeshJoinWorker:
    """The independent MESHJOIN thread, including shed recovery and DW load."""

    def __init__(
        self,
        runtime: RuntimeConfig,
        buffer: PriorityStreamBuffer,
        state: RuntimeState,
        stores: dict[int, tuple[str, str]],
    ) -> None:
        self.runtime = runtime
        self.buffer = buffer
        self.state = state
        self.stores = stores
        self.loaded_to_warehouse = 0
        self.customer_unmatched = 0
        self.product_unmatched = 0
        self.hash_transactions = 0
        self.error: Exception | None = None

    def run(self) -> None:
        lock_connection = None
        customer_pager: MasterDataPager | None = None
        product_pager: MasterDataPager | None = None
        customer_pass: MeshJoinPass | None = None
        product_pass: MeshJoinPass | None = None
        loader: WarehouseLoader | None = None
        shed: ShedRepository | None = None
        try:
            lock_connection = connect_database(self.runtime.database)
            with lock_connection.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(%s)", (1_146_569_289,))
                acquired = bool(cursor.fetchone()[0])
            lock_connection.commit()
            if not acquired:
                raise RuntimeError(
                    "Another MESHJOIN worker already holds the database runtime lock."
                )

            customer_pager = MasterDataPager(
                self.runtime.database,
                "staging_area.customer_master",
                "customer_id",
                "customer_id, gender, age_group, occupation, city_category, "
                "stay_in_current_city_years, marital_status",
                self.runtime.master_partition_size,
            )
            product_pager = MasterDataPager(
                self.runtime.database,
                "staging_area.product_master",
                "product_id",
                "product_id, product_name, product_category, supplier_id, supplier_name",
                self.runtime.master_partition_size,
            )
            customer_pass = MeshJoinPass(
                customer_pager,
                lambda transaction: transaction.customer_id,
                lambda master: int(master[0]),
                self._enrich_customer,
                lambda transaction: transaction.customer_complete,
            )
            product_pass = MeshJoinPass(
                product_pager,
                lambda transaction: transaction.product_id,
                lambda master: str(master[0]),
                self._enrich_product,
                lambda transaction: transaction.product_complete,
            )
            loader = WarehouseLoader(
                self.runtime.database,
                self.stores,
                self.runtime.stream_chunk_size,
            )
            shed = ShedRepository(self.runtime.database)
            recovered_claims = shed.release_stale_claims()
            print(
                "MESHJOIN partitions: "
                f"customer={customer_pager.partition_count}, "
                f"product={product_pager.partition_count}"
            )
            if recovered_claims:
                print(f"Recovered {recovered_claims} uncommitted shed claims.")
            pending_input: list[Transaction] = []
            pending_since: float | None = None
            while not self.state.stop_event.is_set():
                space = self.runtime.stream_chunk_size - len(pending_input)
                if space > 0:
                    received = self.buffer.get_many(space, timeout=0.05)
                    if received:
                        if not pending_input:
                            pending_since = time.monotonic()
                        pending_input.extend(received)

                incoming: list[Transaction] = []
                if len(pending_input) >= self.runtime.stream_chunk_size:
                    incoming = pending_input[: self.runtime.stream_chunk_size]
                    del pending_input[: self.runtime.stream_chunk_size]
                    pending_since = time.monotonic() if pending_input else None
                elif pending_input and pending_since is not None:
                    # The PDF permits a chunk of *up to* 500. A short idle
                    # flush keeps low-rate online data near-real-time while
                    # bursty periodic groups still use full 500-row chunks.
                    if time.monotonic() - pending_since >= 1.0:
                        incoming = pending_input
                        pending_input = []
                        pending_since = None

                customer_ready: list[Transaction] = []
                if incoming or customer_pass.has_pending:
                    customer_ready = customer_pass.step(incoming)

                product_ready: list[Transaction] = []
                if customer_ready or product_pass.has_pending:
                    product_ready = product_pass.step(customer_ready)

                self.hash_transactions = (
                    customer_pass.active_transactions + product_pass.active_transactions
                )

                for transaction in product_ready:
                    loader.stage(transaction)

                if not self.state.is_peak():
                    loader.drain_cache(self.runtime.stream_chunk_size)
                self.loaded_to_warehouse = loader.loaded_count

                if not self.state.is_peak() and self.buffer.qsize() < self.buffer.maxsize // 4:
                    shed.load_when_idle(self.buffer, self.runtime.stream_chunk_size)

        except Exception as error:
            self.error = error
            self.state.stop()
        finally:
            if customer_pass is not None:
                self.customer_unmatched = customer_pass.unmatched
            if product_pass is not None:
                self.product_unmatched = product_pass.unmatched
            self.hash_transactions = sum(
                active.active_transactions
                for active in (customer_pass, product_pass)
                if active is not None
            )
            if loader is not None:
                try:
                    loader.close()
                    self.loaded_to_warehouse = loader.loaded_count
                except Exception as cleanup_error:
                    if self.error is None:
                        self.error = cleanup_error
            if shed is not None:
                shed.close()
            if customer_pager is not None:
                customer_pager.close()
            if product_pager is not None:
                product_pager.close()
            if lock_connection is not None:
                lock_connection.close()

    @staticmethod
    def _enrich_customer(transaction: Transaction, master: tuple) -> None:
        (
            _,
            transaction.gender,
            transaction.age_group,
            transaction.occupation,
            transaction.city_category,
            transaction.stay_in_current_city_years,
            transaction.marital_status,
        ) = master

    @staticmethod
    def _enrich_product(transaction: Transaction, master: tuple) -> None:
        (
            _,
            transaction.product_name,
            transaction.product_category,
            transaction.supplier_id,
            transaction.supplier_name,
        ) = master
