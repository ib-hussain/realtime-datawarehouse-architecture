import csv
import tempfile
import unittest
from pathlib import Path

from meshjoin.stream_buffer import PriorityStreamBuffer
from meshjoin.streamer import OnlineThread, PeriodicThread, RuntimeState


class GroupCursor:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, _sql):
        return None

    def fetchall(self):
        return [(3,), (7,)]


class GroupConnection:
    def __init__(self):
        self.committed = False

    def cursor(self):
        return GroupCursor()

    def commit(self):
        self.committed = True


class StreamerTests(unittest.TestCase):
    def test_online_file_rows_receive_stable_source_ids(self):
        fields = (
            "transaction_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "event_time",
            "sales_mode",
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "online.csv"
            with source.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                for number in (1, 2):
                    writer.writerow(
                        {
                            "transaction_id": f"INV{number}",
                            "customer_id": "12345",
                            "product_id": f"P{number}",
                            "quantity": "1",
                            "unit_price": "2.50",
                            "event_time": "01/12/2010 08:26",
                            "sales_mode": "true",
                        }
                    )

            state = RuntimeState("peak")
            buffer = PriorityStreamBuffer(10)
            producer = OnlineThread(source, buffer, state, 10_000, 10_000)
            producer.start()
            producer.join(timeout=2)

        self.assertFalse(producer.is_alive())
        self.assertIsNone(producer.error)
        rows = buffer.get_many(10)
        self.assertEqual([row.source_row_id for row in rows], [1, 2])

    def test_runtime_state_rejects_unknown_rate_mode(self):
        with self.assertRaises(ValueError):
            RuntimeState("turbo")

    def test_periodic_resume_lists_only_database_pending_groups(self):
        state = RuntimeState("peak")
        producer = PeriodicThread(
            database=None,
            buffer=PriorityStreamBuffer(10),
            state=state,
            simulated_hour_seconds=60,
            group_count=48,
        )
        connection = GroupConnection()

        self.assertEqual(producer._pending_groups(connection), [3, 7])
        self.assertTrue(connection.committed)


if __name__ == "__main__":
    unittest.main()
