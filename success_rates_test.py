"""
pytest is often used for testing in the Python industry. However, I personally dislike most of the pytest features,
and I find the pytest stack traces/error messages unreadable. So, for this project, I am using the builtin Python unittest.
"""

import unittest
from typing import Any

import duckdb
from success_rates import get_success_rates
from exceptions import TableNotFoundError, InvalidBucketSizeError

class TestGetSuccessRates(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Creates an in-memory DuckDB connection and table with sample data without using Pandas.
        """
        cls.db_connection = duckdb.connect(database=':memory:')
        cls.db_connection.execute("""
            CREATE TABLE interview_table (
                vehicle_type TEXT,
                distance INTEGER,
                detection BOOLEAN
            )
        """)

        # Insert minimal test data
        cls.db_connection.execute("""
            INSERT INTO interview_table (vehicle_type, distance, detection) VALUES
            ('car', 5, TRUE),
            ('car', 15, FALSE),
            ('car', 25, TRUE),
            ('car', 35, FALSE),
            ('truck', 8, FALSE),
            ('truck', 18, TRUE),
            ('truck', 28, FALSE),
            ('truck', 38, TRUE)
        """)

        cls.table_names = ["interview_table"]

    @classmethod
    def tearDownClass(cls):
        """Closes the DuckDB in-memory connection after tests run."""
        cls.db_connection.close()

    def test_bucket_size_50(self):
        """Tests get_success_rates() with a bucket size of 50."""
        bucket_size = 50
        result = get_success_rates(self.table_names, bucket_size, self.db_connection)
        expected_result = _sort_by_vehicle_type([
            {
                "vehicle_type": "car",
                "from1to50": 0.5, "from51to100": None
            },
            {
                "vehicle_type": "truck",
                "from1to50": 0.5, "from51to100": None
            }
        ])
        self.assertEqual(_sort_by_vehicle_type(result), expected_result)

    def test_bucket_size_30(self):
        """Tests get_success_rates() with a bucket size of 30."""
        bucket_size = 30
        result = get_success_rates(self.table_names, bucket_size, self.db_connection)
        expected_result = _sort_by_vehicle_type([
            {
                "vehicle_type": "car",
                "from1to30": 2/3, "from31to60": 0, "from61to90": None, "from91to100": None
            },
            {
                "vehicle_type": "truck",
                "from1to30": 1/3, "from31to60": 1, "from61to90": None, "from91to100": None
            }
        ])
        self.assertEqual(_sort_by_vehicle_type(result), expected_result)

    def test_empty_table(self):
        """Tests get_success_rates() with an empty table."""
        self.db_connection.execute("CREATE TABLE empty_table AS SELECT * FROM interview_table WHERE 1=0;")
        result = get_success_rates(["empty_table"], 50, self.db_connection)
        self.assertEqual(result, [])

    def test_invalid_bucket_size(self):
        """Tests get_success_rates() with an invalid bucket size."""
        with self.assertRaises(InvalidBucketSizeError):
            get_success_rates(self.table_names, -1, self.db_connection)
        with self.assertRaises(InvalidBucketSizeError):
            get_success_rates(self.table_names, 200, self.db_connection)

    def test_table_not_found(self):
        """Tests get_success_rates() when a non-existent table is requested."""
        with self.assertRaises(TableNotFoundError):
            get_success_rates(["non_existent_table"], 50, self.db_connection)

def _sort_by_vehicle_type(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: row["vehicle_type"])

if __name__ == "__main__":
    unittest.main()
