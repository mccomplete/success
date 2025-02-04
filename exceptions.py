from duckdb.duckdb import CatalogException


class TableNotFoundError(Exception):
    """Raised when a specified table does not exist in the database."""
    pass

class FileNotFoundError(Exception):
    """Raised when a specified file does not exist when using DuckDB without a connection."""
    pass

class UnsupportedFileFormatError(Exception):
    """Raised when a specified file format is not supported by DuckDB."""
    pass

class InvalidBucketSizeError(Exception):
    """Raised when the user passes an invalid bucket size."""

def handle_duckdb_exceptions(func):
    # todo: hard code the args list here
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except CatalogException as e:
            table_names, db_connection = args[0], args[2] if len(args) > 2 else kwargs.get("db_connection")
            if db_connection:
                raise TableNotFoundError(f"One or more tables in {table_names} do not exist.") from e
            else:
                raise FileNotFoundError(
                    f"One or more files in {table_names} do not exist or are in an unsupported format."
                ) from e

    return wrapper
