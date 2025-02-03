def generate_success_rates_sql(bucket_size: int = 10) -> str:
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

    sql_parts = [
        # Example: Generates "AVG(CASE WHEN distance BETWEEN 1 AND 10 THEN detection::int END) AS from1to10"
        f"AVG(CASE WHEN distance BETWEEN {start} AND {end} THEN detection::int END) AS from{start}to{end}"
        for start, end in buckets
    ]

    sql_query = f"SELECT\n    " + ",\n    ".join(sql_parts) + "\nFROM interview_table;"

    return sql_query
