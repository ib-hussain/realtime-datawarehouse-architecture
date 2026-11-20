import csv
import tempfile
import unittest
from pathlib import Path

from data.dynamic.clean_data import run as clean
from data.dynamic.data_split import run as split
from data.static.data_split import sales_mode

FIELDS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


class DynamicTests(unittest.TestCase):
    def test_clean_split_determinism(self):
        with tempfile.TemporaryDirectory() as t:
            d = Path(t)
            raw = d / "raw.csv"
            good = {
                "InvoiceNo": "10001",
                "StockCode": "ABC",
                "Description": "Product",
                "Quantity": "3",
                "InvoiceDate": "01/12/2010 08:26",
                "UnitPrice": "2.50",
                "CustomerID": "12345",
                "Country": "United Kingdom",
            }
            canceled = {**good, "InvoiceNo": "C10002"}
            missing = {**good, "CustomerID": ""}
            with raw.open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=FIELDS)
                w.writeheader()
                w.writerows([good, good, canceled, missing])
            stats = clean(raw, d / "cleaned.csv")
            self.assertEqual(stats["raw_rows"], 4)
            self.assertEqual(stats["clean_rows"], 1)
            self.assertEqual(stats["duplicates_removed"], 1)
            result = split(d / "cleaned.csv", d)
            self.assertEqual(result["online_rows"] + result["periodic_rows"], 1)
            self.assertEqual(result["online_rows"], int(sales_mode("10001")))


if __name__ == "__main__":
    unittest.main()
