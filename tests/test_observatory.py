import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import observatory


class FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class FakeConnection:
    def __init__(self):
        self.closed = False

    def cursor(self):
        return FakeCursor()

    def close(self):
        self.closed = True


class ObservatoryTests(unittest.TestCase):
    def test_snapshot_reports_missing_relations_instead_of_querying_them(self):
        connection = FakeConnection()
        relation_map = {name: False for name in observatory.RUNTIME_RELATIONS}

        with (
            patch.object(observatory, "database", return_value=connection),
            patch.object(observatory, "relation_status", return_value=relation_map),
        ):
            result = observatory.snapshot()

        self.assertFalse(result["schema_ready"])
        self.assertEqual(result["cached_rows"], 0)
        self.assertIn("data_warehouse.live_cache", result["missing_relations"])
        self.assertTrue(connection.closed)

    def test_stale_pid_file_is_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            pid_path = Path(directory) / "runtime.json"
            pid_path.write_text(json.dumps({"pid": 99999999}), encoding="utf-8")
            with (
                patch.object(observatory, "PID_PATH", pid_path),
                patch.object(observatory.os, "kill", side_effect=ProcessLookupError),
            ):
                self.assertFalse(observatory.is_running())
            self.assertFalse(pid_path.exists())

    def test_tail_validates_line_count(self):
        with self.assertRaises(ValueError):
            observatory.tail(0)

    def test_runtime_info_includes_atomic_worker_telemetry(self):
        with tempfile.TemporaryDirectory() as directory:
            telemetry_path = Path(directory) / "telemetry.json"
            telemetry_path.write_text(
                json.dumps({"source_mode": "both", "arrival_mode": "peak"}),
                encoding="utf-8",
            )
            with (
                patch.object(observatory, "TELEMETRY", telemetry_path),
                patch.object(observatory, "is_running", return_value=True),
                patch.object(observatory, "_read_pid_record", return_value={"pid": 123}),
            ):
                result = observatory.runtime_info()

        self.assertTrue(result["running"])
        self.assertEqual(result["telemetry"]["source_mode"], "both")

    def test_query_rejects_multiple_and_high_risk_selects_before_connecting(self):
        blocked = (
            "SELECT 1; SELECT 2",
            "WITH removed AS (DELETE FROM data_warehouse.transactions RETURNING *) SELECT * FROM removed",
            "SELECT * FROM pg_catalog.pg_shadow",
            "SELECT pg_read_file('/etc/passwd')",
            "SELECT \"pg_read_file\"('/etc/passwd')",
            'SELECT "pg_catalog" . "pg_read_file"(\'/etc/passwd\')',
        )
        with patch.object(observatory, "database") as database:
            for sql in blocked:
                with self.subTest(sql=sql), self.assertRaises(ValueError):
                    observatory.query(sql)
        database.assert_not_called()


if __name__ == "__main__":
    unittest.main()
