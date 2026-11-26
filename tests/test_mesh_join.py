import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from meshjoin.base import DatabaseConfig, RuntimeConfig
from meshjoin.mesh_join import MeshJoinWorker, WarehouseLoader
from meshjoin.stream_buffer import PriorityStreamBuffer, Transaction
from meshjoin.streamer import RuntimeState


class FakeCursor:
    def __init__(self):
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, _parameters=None):
        if "RETURNING time_key" in sql:
            self.result = (11,)
        elif "RETURNING product_store_key" in sql:
            self.result = (22,)

    def fetchone(self):
        return self.result


class FakeConnection:
    def cursor(self):
        return FakeCursor()


class LockCursor:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, _sql, _parameters=None):
        return None

    def fetchone(self):
        return (False,)


class LockConnection:
    def __init__(self):
        self.closed = False
        self.committed = False

    def cursor(self):
        return LockCursor()

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class FlushCursor:
    def __init__(self):
        self.statements = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def executemany(self, sql, parameters):
        self.statements.append(sql)
        self.rowcount = len(parameters)


class FlushConnection:
    def __init__(self):
        self.cursor_instance = FlushCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        raise AssertionError("flush should not roll back in this test")


class WarehouseLoaderTests(unittest.TestCase):
    def test_periodic_stage_retains_idempotency_identity(self):
        transaction = Transaction(
            source="periodic",
            source_row_id=42,
            transaction_id="INV-42",
            event_time=datetime(2011, 1, 2, 3, 4),
            customer_id=12345,
            product_id="PRODUCT-1",
            quantity=2,
            unit_price=Decimal("3.50"),
            sales_mode=False,
            gender="Female",
            age_group="26-35",
            occupation="Engineer",
            city_category="A",
            stay_in_current_city_years="2",
            marital_status="Single",
            product_name="Product",
            product_category="Other",
            supplier_id="SUP01",
            supplier_name="Northwind Supplies",
        )

        with patch("meshjoin.mesh_join.connect_database", return_value=FakeConnection()):
            loader = WarehouseLoader(
                database=None,
                stores={
                    2: ("London Bridge", "London"),
                    3: ("Manchester Arndale", "Manchester"),
                    4: ("Birmingham Bullring", "Birmingham"),
                    5: ("Edinburgh Princes", "Edinburgh"),
                },
                flush_size=100,
            )
            loader.stage(transaction)

        staged = loader._facts[0]
        self.assertEqual(staged[1:3], ("periodic", 42))
        self.assertEqual(loader._periodic_source_ids, [42])

    def test_worker_refuses_a_second_database_runtime(self):
        database = DatabaseConfig("localhost", 5432, "dwbi", "postgres", "")
        runtime = RuntimeConfig(database=database, online_file=Path("online.csv"))
        state = RuntimeState("peak")
        worker = MeshJoinWorker(runtime, PriorityStreamBuffer(1_000), state, {})
        connection = LockConnection()

        with patch("meshjoin.mesh_join.connect_database", return_value=connection):
            worker.run()

        self.assertIsInstance(worker.error, RuntimeError)
        self.assertIn("already holds", str(worker.error))
        self.assertTrue(state.stop_event.is_set())
        self.assertTrue(connection.committed)
        self.assertTrue(connection.closed)

    def test_cache_insert_casts_postgresql_enum_parameters(self):
        connection = FlushConnection()
        with patch("meshjoin.mesh_join.connect_database", return_value=connection):
            loader = WarehouseLoader(
                database=None,
                stores={1: ("Online", "Online")},
                flush_size=100,
            )
            loader._online_cache.append(
                (
                    "INV-1",
                    "online",
                    1,
                    12345,
                    "PRODUCT-1",
                    "SUP01",
                    1,
                    11,
                    22,
                    datetime(2011, 1, 2, 3, 4),
                    2,
                    Decimal("3.50"),
                    Decimal("7.00"),
                    True,
                    "Immediate",
                )
            )
            loader.flush()

        sql = connection.cursor_instance.statements[0]
        self.assertIn("::data_warehouse.supplier_id_enum", sql)
        self.assertIn("::data_warehouse.processing_mode_enum", sql)
        self.assertTrue(connection.committed)


if __name__ == "__main__":
    unittest.main()
