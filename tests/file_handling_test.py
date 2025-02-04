import unittest
from typing import Any

import os
import tempfile
import pandas as pd

from success.exceptions import UnsupportedFileFormatError
from success.success_rates import get_success_rates

class TestSuccessRatesFileHandling(unittest.TestCase):
    def setUp(self):
        """Set up temporary Parquet files for testing."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.parquet_files = [os.path.join(self.temp_dir.name, f"test_data_{i}.parquet") for i in range(3)]

        # Create sample data
        data = pd.DataFrame({
            "vehicle_type": ["car", "bike", "truck"],
            "distance": [10, 55, 90],
            "detection": [1, 0, 1]
        })

        # Write data to multiple Parquet files
        for file_path in self.parquet_files:
            data.to_parquet(file_path)

    def tearDown(self):
        """Clean up temporary files after each test."""
        self.temp_dir.cleanup()

    def test_non_existent_file(self):
        """Test case where one of the files does not exist."""
        invalid_files = ["non_existent_file.parquet"]

        with self.assertRaises(FileNotFoundError) as context:
            get_success_rates(invalid_files)

    def test_unsupported_file_format(self):
        """Test case where one of the files is in an unsupported format."""
        tmp_file = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        tmp_file.write(b"This is a test file, but not in a valid format.")
        tmp_file.close()

        try:
            with self.assertRaises(UnsupportedFileFormatError) as context:
                get_success_rates([tmp_file.name])
        finally:
            os.remove(tmp_file.name)

    def test_multiple_parquet_files(self):
        """Test case where multiple valid Parquet files are provided with a bucket size of 50."""
        result = get_success_rates(self.parquet_files, bucket_size=50)
        expected_result = _sort_by_vehicle_type([
            {
                "vehicle_type": "car",
                "from1to50": 1.0, "from51to100": None
            },
            {
                "vehicle_type": "truck",
                "from1to50": None, "from51to100": 1.0
            },
            {
                "vehicle_type": "bike",
                "from1to50": None, "from51to100": 0.0
            }
        ])
        self.assertEqual(_sort_by_vehicle_type(result), expected_result)

def _sort_by_vehicle_type(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: row["vehicle_type"])

if __name__ == "__main__":
    unittest.main()
