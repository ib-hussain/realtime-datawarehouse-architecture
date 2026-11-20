import os
import tempfile
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from cli_app import DEFAULT_DYNAMIC, DEFAULT_RAW, build_parser, main
from meshjoin.base import load_runtime_config


class CliDefaultsTests(unittest.TestCase):
    def test_clean_and_prepare_have_project_defaults(self):
        parser = build_parser()
        clean = parser.parse_args(["clean"])
        prepare = parser.parse_args(["prepare"])

        self.assertEqual(clean.input_path, DEFAULT_RAW)
        self.assertEqual(clean.output, DEFAULT_DYNAMIC / "processed-retail.csv")
        self.assertEqual(prepare.input_path, DEFAULT_RAW)
        self.assertEqual(prepare.output_dir, DEFAULT_DYNAMIC)

    def test_optional_input_flag_is_supported(self):
        parser = build_parser()
        args = parser.parse_args(["clean", "--input", "custom.csv"])
        self.assertEqual(args.input_option, Path("custom.csv"))

    def test_driver_errors_are_concise_unless_debug_is_requested(self):
        error_output = StringIO()
        with (
            patch("cli_app.execute", side_effect=Exception("database unavailable")),
            redirect_stderr(error_output),
        ):
            exit_code = main(["status"])

        self.assertEqual(exit_code, 1)
        self.assertEqual(error_output.getvalue(), "ERROR: database unavailable\n")


class RuntimeConfigTests(unittest.TestCase):
    def test_safe_defaults_work_without_env_file(self):
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.env"
            with patch.dict(os.environ, {}, clear=True):
                config = load_runtime_config(missing)

        self.assertEqual(config.database.host, "localhost")
        self.assertEqual(config.database.port, 5432)
        self.assertEqual(config.database.database, "realtime_data_stream")
        self.assertEqual(config.database.user, "postgres")
        self.assertEqual(config.database.password, "")
        self.assertEqual(config.stream_buffer_limit, 1000)

    def test_invalid_buffer_relationship_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text(
                "STREAM_CHUNK_SIZE=500\nSTREAM_BUFFER_LIMIT=100\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(ValueError, "STREAM_BUFFER_LIMIT"):
                    load_runtime_config(env_file)


if __name__ == "__main__":
    unittest.main()
