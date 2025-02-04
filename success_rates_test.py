import unittest
from unittest import TestCase
import duckdb

from src.success_rates import get_success_rates


class TestGetSuccessRates(TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Creates an in-memory DuckDB connection and table with sample data. The table includes only relevant columns.
        """
        cls.db_connection = duckdb.connect(database=':memory:')
        cls.db_connection.execute("""
            CREATE TABLE interview_table (
                vehicle_type TEXT,
                distance INTEGER,
                detection BOOLEAN
            )
        """)

        cls.db_connection.execute("""
            INSERT INTO interview_table (vehicle_type, distance, detection) VALUES
            ('car', 5, TRUE),
            ('truck', 15, FALSE),
            ('car', 25, TRUE),
            ('truck', 35, FALSE)
        """)

        cls.table_names = ["interview_table"]

    @classmethod
    def tearDownClass(cls):
        """Closes the DuckDB in-memory connection after tests run."""
        cls.db_connection.close()

    def test_get_success_rates(self):
        """Tests get_success_rates() function with an in-memory DuckDB table."""
        bucket_size = 10
        result = get_success_rates(self.table_names, bucket_size, self.db_connection)

        self.assertIsInstance(result, list)
        self.assertTrue(all(isinstance(row, dict) for row in result))
        self.assertIn("vehicle_type", result[0])

        # Ensure the expected structure of keys based on bucket_size
        expected_keys = {"vehicle_type"} | {f"from{i}to{min(i+bucket_size-1, 100)}" for i in range(1, 101, bucket_size)}
        self.assertEqual(set(result[0].keys()), expected_keys)

if __name__ == "__main__":
    unittest.main()
