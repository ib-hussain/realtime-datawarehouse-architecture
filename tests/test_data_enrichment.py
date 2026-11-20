import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data_enrichment import DataEnrichmentApplication
from meshjoin.base import DatabaseConfig, RuntimeConfig


class RuntimeTelemetryTests(unittest.TestCase):
    def test_telemetry_is_atomic_serializable_and_reflects_commands(self):
        runtime = RuntimeConfig(
            database=DatabaseConfig("localhost", 5432, "dwbi", "postgres", ""),
            online_file=Path("online.csv"),
            peak_rate=80,
            non_peak_rate=10,
        )
        with patch("data_enrichment.load_runtime_config", return_value=runtime):
            application = DataEnrichmentApplication("peak")

        application.state.set_source_mode("periodic")
        application.state.set_arrival_mode("non-peak")
        with tempfile.TemporaryDirectory() as directory:
            telemetry_file = Path(directory) / "telemetry.json"
            application.telemetry_file = telemetry_file
            application._write_telemetry()
            payload = json.loads(telemetry_file.read_text(encoding="utf-8"))

        self.assertEqual(payload["source_mode"], "periodic")
        self.assertEqual(payload["arrival_mode"], "non-peak")
        self.assertEqual(payload["rate_per_second"], 10)
        self.assertEqual(payload["stream_buffer_limit"], 1_000)
        self.assertFalse(any(payload["threads"].values()))


if __name__ == "__main__":
    unittest.main()
