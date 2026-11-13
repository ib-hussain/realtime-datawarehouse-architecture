"""Shared priority stream buffer for online, periodic, and shed records."""

from __future__ import annotations

import itertools
import queue
import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class Transaction:
    """One product line in a shopping cart, at the project fact-table grain."""

    source: str
    source_row_id: int | None
    transaction_id: str
    event_time: datetime
    customer_id: int
    product_id: str
    quantity: int
    unit_price: Decimal
    sales_mode: bool
    release_hour: int | None = None
    processing_mode: str = "Immediate"

    gender: str | None = None
    age_group: str | None = None
    occupation: str | None = None
    city_category: str | None = None
    stay_in_current_city_years: str | None = None
    marital_status: str | None = None
    product_name: str | None = None
    product_category: str | None = None
    supplier_id: str | None = None
    supplier_name: str | None = None

    @property
    def revenue(self) -> Decimal:
        return self.unit_price * self.quantity

    @property
    def customer_complete(self) -> bool:
        return self.gender is not None

    @property
    def product_complete(self) -> bool:
        return self.supplier_id is not None


class PriorityStreamBuffer:
    """A bounded queue where online rows outrank periodic and shed rows."""

    ONLINE_PRIORITY = 0
    PERIODIC_PRIORITY = 1
    SHED_PRIORITY = 2

    def __init__(self, maxsize: int = 1_000) -> None:
        self._queue: queue.PriorityQueue[tuple[int, int, Transaction]] = queue.PriorityQueue(
            maxsize=maxsize
        )
        self._sequence = itertools.count()

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    def qsize(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()

    def try_put(self, transaction: Transaction) -> bool:
        priority = self._priority_for(transaction)
        try:
            self._queue.put_nowait((priority, next(self._sequence), transaction))
            return True
        except queue.Full:
            return False

    def put(
        self,
        transaction: Transaction,
        stop_event: threading.Event,
        timeout: float = 0.2,
    ) -> bool:
        priority = self._priority_for(transaction)
        while not stop_event.is_set():
            try:
                self._queue.put((priority, next(self._sequence), transaction), timeout=timeout)
                return True
            except queue.Full:
                continue
        return False

    def get_many(
        self,
        maximum: int,
        timeout: float = 0.1,
    ) -> list[Transaction]:
        if maximum <= 0:
            return []
        transactions: list[Transaction] = []
        try:
            _, _, first = self._queue.get(timeout=timeout)
            transactions.append(first)
        except queue.Empty:
            return transactions
        while len(transactions) < maximum:
            try:
                _, _, transaction = self._queue.get_nowait()
                transactions.append(transaction)
            except queue.Empty:
                break
        return transactions

    def _priority_for(self, transaction: Transaction) -> int:
        if transaction.source == "online":
            return self.ONLINE_PRIORITY
        if transaction.source == "shed":
            return self.SHED_PRIORITY
        return self.PERIODIC_PRIORITY

