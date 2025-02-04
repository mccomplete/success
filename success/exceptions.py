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
    pass
