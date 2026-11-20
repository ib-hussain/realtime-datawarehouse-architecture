import hashlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data.dataFeeder import parse_args
from data.static.feed_db import file_sha256


class DataFeederCliTests(unittest.TestCase):
    def test_dynamic_arguments_are_declared(self):
        with patch.object(
            sys,
            "argv",
            [
                "dataFeeder.py",
                "--dynamic-source",
                "source.csv",
                "--dynamic-output-dir",
                "generated",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.dynamic_source, Path("source.csv"))
        self.assertEqual(args.dynamic_output_dir, Path("generated"))

    def test_file_manifest_digest_is_streamed_correctly(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.csv"
            source.write_bytes(b"abc\n123\n")
            expected = hashlib.sha256(b"abc\n123\n").hexdigest()
            self.assertEqual(file_sha256(source), expected)


if __name__ == "__main__":
    unittest.main()
