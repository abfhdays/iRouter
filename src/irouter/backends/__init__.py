"""Backend execution engines."""
from irouter.backends.base import BaseBackend
from irouter.backends.duckdb_backend import DuckDBBackend

__all__ = ["BaseBackend", "DuckDBBackend"]