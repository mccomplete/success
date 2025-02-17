# Success Rates Calculation

## Overview
This project calculates success rates based on distance buckets from multiple data sources using DuckDB.

## Features
- Reads multiple Parquet files as input (or any file type supported by DuckDB)
- Supports passing a DuckDB connection and a table name (or list of table names)
- Supports configurable bucket sizes for distance calculations
- Returns success rates based on vehicle types and distance ranges
- Handles missing files and unsupported formats gracefully

## Installation

Run:

```sh
pip install git+https://github.com/mccomplete/success.git
```

This will fetch the latest version from the repository and install it as a Python package.

To verify the installation, run:
```sh
python -c "import success; print(success)"
```

If no errors occur, the package has been successfully installed.

## Usage

### Function: `get_success_rates`
#### Parameters:
```python
get_success_rates(
    table_names: list[str],
    bucket_size: int = 10,
    db_connection: Optional[DuckDBPyConnection] = None
) -> list[dict[str, Any]]
```
- `table_names`: List of file paths to read data from (Parquet files required)
- `bucket_size`: Size of distance buckets (default: 10, must be between 1 and 100)
- `db_connection`: Optional DuckDB connection

#### Example:
```python
from success_rates import get_success_rates

result = get_success_rates(["data1.parquet", "data2.parquet"], bucket_size=50)
print(json.dumps(result, indent=4))
```

## Error Handling
- **`FileNotFoundError`**: Raised if any input file does not exist.
- **`InvalidBucketSizeError`**: Raised if the `bucket_size` is outside the allowed range (1-100).
- **`TableNotFoundError`**: Raised when a referenced table is missing.
- **`UnsupportedFileFormatError`** Raised when one or more of the input files is in a format that is unsupported by DuckDB.

## SQL Query Generation with `generate_union_table_expression`
The `generate_union_table_expression` function constructs a SQL fragment that allows users to query multiple DuckDB-supported files as if they were a single table. It generates a `UNION ALL` SQL expression from a list of file paths.

### Function: `generate_union_table_expression`
#### Parameters:
```python
def generate_union_table_expression(table_names: list[str]) -> str:
```
- `table_names`: List of file paths to be included in the SQL query.

#### Example Usage:
```python
table_names = ["data1.parquet", "data2.parquet"]
query = f"""
SELECT vehicle_type, AVG(distance) AS avg_distance
FROM {generate_union_table_expression(table_names)}
GROUP BY vehicle_type;
"""
```
This function enables querying multiple files, treating them as a single logical table (`interview_table`). The generated SQL can be used in DuckDB to execute queries against multiple data sources.

## Input and Output Schema

### Input Schema
The input must have with the following columns 
(if there are additional columns in the input, they are ignored):

| Column Name   | Type   | Description |
|--------------|--------|-------------|
| `vehicle_type` | `STRING` | The type of vehicle (e.g., car, truck, bike). |
| `distance`    | `INTEGER` | The distance from the camera to the target vehicle. |
| `detection`   | `BOOL` | Boolean (true or false) representing detection success. |

Each input file must conform to this schema to ensure correct processing.

### Output Schema
The function returns a list of dictionaries with aggregated success rates per vehicle type and distance bucket.

| Column Name    | Type    | Description |
|--------------|---------|-------------|
| `vehicle_type` | `STRING` | The type of vehicle. |
| `fromXtoY`    | `FLOAT`  | The success rate (percentage of rows where `detection` is true) for each distance bucket. |

Example output with `bucket_size=50`:
```json
[
    {"vehicle_type": "car", "from1to50": 0.75, "from51to100": 0.60},
    {"vehicle_type": "truck", "from1to50": 0.80, "from51to100": 0.50}
]
```
