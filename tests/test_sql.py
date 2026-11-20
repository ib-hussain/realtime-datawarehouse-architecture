import unittest
from pathlib import Path

try:
    from pglast import parse_sql
except ImportError:  # pragma: no cover - optional development dependency.
    parse_sql = None


@unittest.skipIf(parse_sql is None, "pglast is installed from requirements-dev.txt")
class PostgreSqlSyntaxTests(unittest.TestCase):
    def test_project_sql_files_parse(self):
        root = Path(__file__).resolve().parents[1]
        expected_minimums = {
            "src/warehouse-schema.sql": 50,
            "src/queries.sql": 20,
            "src/checks.sql": 5,
        }
        for relative_path, minimum in expected_minimums.items():
            with self.subTest(path=relative_path):
                sql = (root / relative_path).read_text(encoding="utf-8")
                self.assertGreaterEqual(len(parse_sql(sql)), minimum)


if __name__ == "__main__":
    unittest.main()
