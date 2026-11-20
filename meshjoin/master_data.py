"""Disk-page readers for master data and the five-row store reference list."""

from __future__ import annotations

from dataclasses import dataclass

from meshjoin.base import DatabaseConfig, connect_database


@dataclass(frozen=True)
class MasterPage:
    rows: list[tuple]
    page_number: int
    partition_count: int


class MasterDataPager:
    """Cyclically reads one 500-row master-data page per MESHJOIN iteration."""

    def __init__(
        self,
        database: DatabaseConfig,
        table_name: str,
        order_column: str,
        columns: str,
        page_size: int = 500,
    ) -> None:
        self._connection = connect_database(database)
        self.table_name = table_name
        self.order_column = order_column
        self.columns = columns
        self.page_size = page_size
        self.next_page_number = 0

        with self._connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            row_count = int(cursor.fetchone()[0])
        if row_count == 0:
            self._connection.close()
            raise ValueError(f"Master-data table {self.table_name} is empty.")
        self.row_count = row_count
        self.partition_count = (row_count + page_size - 1) // page_size

    def next_page(self) -> MasterPage:
        page_number = self.next_page_number
        offset = page_number * self.page_size
        query = (
            f"SELECT {self.columns} FROM {self.table_name} "
            f"ORDER BY {self.order_column} LIMIT %s OFFSET %s"
        )
        with self._connection.cursor() as cursor:
            cursor.execute(query, (self.page_size, offset))
            rows = list(cursor.fetchall())
        self.next_page_number = (page_number + 1) % self.partition_count
        return MasterPage(rows, page_number, self.partition_count)

    def close(self) -> None:
        self._connection.close()


def load_stores(database: DatabaseConfig) -> dict[int, tuple[str, str]]:
    """Store data is small and may be held in memory outside MESHJOIN."""

    connection = connect_database(database)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT store_id, store_name, city FROM staging_area.store_list ORDER BY store_id"
            )
            rows = cursor.fetchall()
        stores = {int(store_id): (str(name), str(city)) for store_id, name, city in rows}
        if set(stores) != {1, 2, 3, 4, 5}:
            raise ValueError("staging_area.store_list must contain exactly stores 1 through 5.")
        return stores
    finally:
        connection.close()
