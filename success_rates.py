from typing import Any, Optional

import duckdb
from duckdb import DuckDBPyRelation, DuckDBPyConnection

from constants import DEFAULT_BUCKET_SIZE, MAX_DISTANCE
from exceptions import InvalidBucketSizeError, TableNotFoundError, UnsupportedFileFormatError


def get_success_rates(
        table_names: list[str], bucket_size: int = DEFAULT_BUCKET_SIZE,
        db_connection: Optional[DuckDBPyConnection] = None
) -> list[dict[str, Any]]:
    query = generate_success_rates_query(table_names, bucket_size)
    try:
        relation = db_connection.sql(query) if db_connection else duckdb.sql(query)
    except duckdb.IOException as e:
        raise FileNotFoundError(str(e))
    except duckdb.CatalogException as e:
        if db_connection:
            raise TableNotFoundError(str(e))
        else:
            raise UnsupportedFileFormatError(str(e))

    return relation_to_json(relation)

def relation_to_json(relation: DuckDBPyRelation) -> list[dict[str, Any]]:
    columns = relation.columns
    data = relation.fetchall()

    json_result = []
    for row in data:
        row_dict = {}
        for i in range(len(columns)):
            row_dict[columns[i]] = row[i]

        json_result.append(row_dict)

    return json_result

def generate_success_rates_query(table_names: list[str], bucket_size: int = 10) -> str:
    """
    Generates an SQL statement to return success rates per distance bucket.
    For example, if bucket_size = 50, returns:
    SELECT
        AVG(CASE WHEN distance BETWEEN 1 AND 50 THEN detection::int END) AS from1to50,
        AVG(CASE WHEN distance BETWEEN 51 AND 100 THEN detection::int END) AS from51to100
    FROM interview_table;
    """
    if bucket_size <= 0 or bucket_size > MAX_DISTANCE:
        raise InvalidBucketSizeError("Bucket size must be between 1 and 100")

    buckets = [(i, min(i + bucket_size - 1, MAX_DISTANCE)) for i in range(1, MAX_DISTANCE + 1, bucket_size)]

    sql_parts = ["vehicle_type"]
    sql_parts += [
        # Example: Generates "AVG(CASE WHEN distance BETWEEN 1 AND 10 THEN detection::int END) AS from1to10"
        f"AVG(CASE WHEN distance BETWEEN {start} AND {end} THEN detection::int END) AS from{start}to{end}"
        for start, end in buckets
    ]

    sql_query = f"SELECT\n    " + ",\n    ".join(sql_parts)
    sql_query += generate_union_table_expression(table_names)
    sql_query += "GROUP BY vehicle_type;"
    return sql_query

def generate_union_table_expression(table_names: list[str]) -> str:
    union_table_expression = "\nFROM ("
    union_parts = [f"SELECT * FROM '{file}'" for file in table_names]
    union_table_expression += "\n    UNION ALL\n    ".join(union_parts) + "\n) AS interview_table "
    return union_table_expression
