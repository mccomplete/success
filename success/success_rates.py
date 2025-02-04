from typing import Any, Optional

import duckdb
from duckdb import DuckDBPyRelation, DuckDBPyConnection

from success.constants import DEFAULT_BUCKET_SIZE, MAX_DISTANCE
from success.exceptions import InvalidBucketSizeError, TableNotFoundError, UnsupportedFileFormatError

def get_success_rates(
        table_names: list[str], bucket_size: int = DEFAULT_BUCKET_SIZE,
        db_connection: Optional[DuckDBPyConnection] = None
) -> list[dict[str, Any]]:
    """
    Computes vehicle detection success rates per distance bucket for the given tables.

    Args:
        table_names (list[str]): If connection is None, this should be a list of Parquet files, or files of any type
            supported by DuckDB. If connection is not None, this should be a list of table names that can be found in
            the given DuckDB database. See the README for more information about the input schema.
        bucket_size (int, optional): Size of each distance bucket. Defaults to DEFAULT_BUCKET_SIZE. Must be less than
            MAX_DISTANCE.
        db_connection (Optional[DuckDBPyConnection], optional): DuckDB connection instance. Defaults to None.

    Returns:
        list[dict[str, Any]]: A list of dictionaries representing the query result in JSON-like format.
        See the README for more information about the output schema.

    Raises:
        FileNotFoundError: If no connection was passed, and one or more of the files cannot be found.
        UnsupportedFileFormatError: If no connection was passed, and one or more of the files is in an unsupported
            format.
        TableNotFoundError: If a connection was passed, and one or more of the tables do not exist in the database.
        InvalidBucketSizeError: If the given bucket size is invalid.
    """
    if bucket_size <= 0 or bucket_size > MAX_DISTANCE:
        raise InvalidBucketSizeError(f"Bucket size must be between 1 and {MAX_DISTANCE}")

    query = _generate_success_rates_query(table_names, bucket_size)
    try:
        relation = db_connection.sql(query) if db_connection else duckdb.sql(query)
    except duckdb.IOException as e:
        raise FileNotFoundError(str(e))
    except duckdb.CatalogException as e:
        if db_connection:
            raise TableNotFoundError(str(e))
        else:
            raise UnsupportedFileFormatError(str(e))

    return _relation_to_json(relation)

def _relation_to_json(relation: DuckDBPyRelation) -> list[dict[str, Any]]:
    """
    Converts a DuckDB relation into a JSON-compatible list of dictionaries.

    Args:
        relation (DuckDBPyRelation): The DuckDB relation to convert.

    Returns:
        list[dict[str, Any]]: A list of dictionaries representing the relation data.
    """
    columns = relation.columns
    data = relation.fetchall()

    json_result = []
    for row in data:
        row_dict = {}
        for i in range(len(columns)):
            row_dict[columns[i]] = row[i]

        json_result.append(row_dict)

    return json_result

def _generate_success_rates_query(table_names: list[str], bucket_size: int = 10) -> str:
    """
    Generates an SQL query to compute vehicle detection success rates per distance bucket.

    For example, if table names = ["my_table"] and bucket_size = 50, returns:
    SELECT
        vehicle_type,
        AVG(CASE WHEN distance BETWEEN 1 AND 50 THEN detection::int END) AS from1to50,
        AVG(CASE WHEN distance BETWEEN 51 AND 100 THEN detection::int END) AS from51to100
    FROM (SELECT * FROM 'my_table') AS interview_table
    GROUP BY vehicle_type;

    Args:
        table_names (list[str]): List of table names (or file names) to include in the query.
        bucket_size (int, optional): Size of each distance bucket. Defaults to DEFAULT_BUCKET_SIZE. Must be less than
            MAX_DISTANCE.

    Returns:
        str: The generated SQL query.
    """
    buckets = [(i, min(i + bucket_size - 1, MAX_DISTANCE)) for i in range(1, MAX_DISTANCE + 1, bucket_size)]

    sql_parts = ["vehicle_type"]
    sql_parts += [
        f"AVG(CASE WHEN distance BETWEEN {start} AND {end} THEN detection::int END) AS from{start}to{end}"
        for start, end in buckets
    ]

    sql_query = f"SELECT\n    " + ",\n    ".join(sql_parts)
    sql_query += "\nFROM " + generate_union_table_expression(table_names)
    sql_query += "\nGROUP BY vehicle_type;"
    return sql_query

def generate_union_table_expression(table_names: list[str]) -> str:
    """
    Creates a SQL fragment that unions multiple tables or files.

    Args:
        table_names (list[str]): List of table or file names.

    Returns:
        str: SQL fragment for the union of all provided tables.
    """
    union_table_expression = "("
    union_parts = [f"SELECT * FROM '{file}'" for file in table_names]
    union_table_expression += "\n    UNION ALL\n    ".join(union_parts) + "\n) AS interview_table "
    return union_table_expression
