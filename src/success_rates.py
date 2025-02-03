from typing import Any

import duckdb
from duckdb import DuckDBPyRelation

def get_success_rates(parquet_files: list[str], bucket_size: int = 10) -> list[dict[str, Any]]:
    query = generate_success_rates_query(parquet_files, bucket_size)
    relation = duckdb.sql(query)
    json_result = relation_to_json(relation)
    return json_result

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

def generate_success_rates_query(parquet_files: list[str], bucket_size: int = 10) -> str:
    """
    Generates an SQL statement to return success rates per distance bucket.
    For example, if bucket_size = 50, returns:
    SELECT
        AVG(CASE WHEN distance BETWEEN 1 AND 50 THEN detection::int END) AS from1to50,
        AVG(CASE WHEN distance BETWEEN 51 AND 100 THEN detection::int END) AS from51to100
    FROM interview_table;
    """
    if bucket_size <= 0 or bucket_size > 100:
        raise ValueError("Bucket size must be between 1 and 100")

    buckets = [(i, min(i + bucket_size - 1, 100)) for i in range(1, 101, bucket_size)]

    sql_parts = ["vehicle_type"]
    sql_parts += [
        # Example: Generates "AVG(CASE WHEN distance BETWEEN 1 AND 10 THEN detection::int END) AS from1to10"
        f"AVG(CASE WHEN distance BETWEEN {start} AND {end} THEN detection::int END) AS from{start}to{end}"
        for start, end in buckets
    ]

    sql_query = f"SELECT\n    " + ",\n    ".join(sql_parts) + "\nFROM ("

    union_parts = [f"SELECT * FROM '{file}'" for file in parquet_files]
    sql_query += "\n    UNION ALL\n    ".join(union_parts) + "\n) AS interview_table "
    sql_query += "GROUP BY vehicle_type;"

    return sql_query
