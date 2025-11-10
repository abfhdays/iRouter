"""Core data types and structures."""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
import sqlglot
"""
Think about each feature of the intelligent query router 
and define data structures that will help manage and represent the core concepts.

1. Backend Enumeration: Define an enumeration for the different backends (DuckDB, Spark, Polars).
2. Table Statistics: Create a data class to hold statistics about tables, including row counts, sizes, and partitioning info.
"""

class Backend(Enum):
    """Available execution backends."""
    DUCKDB = "duckdb"
    SPARK = "spark"
    POLARS = "polars"


@dataclass
class TableStats:
    """Statistics for a table."""
    table_name: str
    row_count: int
    size_bytes: int
    size_gb: float
    num_files: int
    columns: Dict[str, Dict[str, Any]]
    is_partitioned: bool
    partition_key: Optional[str] = None
    num_partitions: Optional[int] = None
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class PartitionInfo:
    """Information about a single partition."""
    path: str
    key: str
    value: str
    size_bytes: int
    row_count: Optional[int] = None


@dataclass
class QueryResult:
    """Result of query execution."""
    data: Any  # DataFrame or similar
    backend_used: Backend
    execution_time_sec: float
    rows_processed: int
    partitions_scanned: int
    total_partitions: int
    from_cache: bool = False
    sql_optimized: Optional[str] = None


@dataclass
class CostEstimate:
    """Cost estimation for a query on a backend."""
    backend: Backend
    estimated_time_sec: float
    estimated_memory_gb: float
    scan_cost: float
    compute_cost: float
    overhead_cost: float
    reasoning: str