"""Core data types and structures."""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class Backend(Enum):
    """Available execution backends."""
    DUCKDB = "duckdb"
    SPARK = "spark"
    POLARS = "polars"


class PredicateOperator(Enum):
    """SQL comparison operators."""
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


@dataclass
class Predicate:
    """
    A single predicate from a WHERE clause.
    
    Example: "date >= '2024-11-01'" becomes:
        Predicate(
            column="date",
            operator=PredicateOperator.GTE,
            value="2024-11-01",
            sql_type="DATE"
        )
    """
    column: str
    operator: PredicateOperator
    value: Any
    sql_type: Optional[str] = None
    
    def evaluate(self, partition_value: Any) -> bool:
        """
        Check if partition value satisfies this predicate.
        
        Args:
            partition_value: Value from partition directory name
            
        Returns:
            True if predicate matches, False otherwise
        """
        # Type conversion based on sql_type
        compare_value = self.value
        
        if self.sql_type:
            partition_value, compare_value = self._convert_types(
                partition_value, 
                self.value
            )
        
        # Evaluate based on operator
        try:
            if self.operator == PredicateOperator.EQ:
                return partition_value == compare_value
            elif self.operator == PredicateOperator.NEQ:
                return partition_value != compare_value
            elif self.operator == PredicateOperator.GT:
                return partition_value > compare_value
            elif self.operator == PredicateOperator.GTE:
                return partition_value >= compare_value
            elif self.operator == PredicateOperator.LT:
                return partition_value < compare_value
            elif self.operator == PredicateOperator.LTE:
                return partition_value <= compare_value
            elif self.operator == PredicateOperator.IN:
                return partition_value in compare_value
            elif self.operator == PredicateOperator.NOT_IN:
                return partition_value not in compare_value
            else:
                return True  # Conservative: include partition
        except Exception:
            return True  # If comparison fails, be conservative
    
    def _convert_types(self, partition_value: Any, compare_value: Any):
        """Convert values to appropriate types for comparison."""
        from datetime import datetime
        
        sql_type_upper = self.sql_type.upper() if self.sql_type else ""
        
        # Date/datetime conversion
        if "DATE" in sql_type_upper or "TIMESTAMP" in sql_type_upper:
            if isinstance(partition_value, str):
                partition_value = datetime.fromisoformat(partition_value)
            if isinstance(compare_value, str):
                compare_value = datetime.fromisoformat(compare_value)
        
        # Numeric conversion
        elif "INT" in sql_type_upper or "BIGINT" in sql_type_upper:
            partition_value = int(partition_value)
            compare_value = int(compare_value)
        
        elif "FLOAT" in sql_type_upper or "DOUBLE" in sql_type_upper or "DECIMAL" in sql_type_upper:
            partition_value = float(partition_value)
            compare_value = float(compare_value)
        
        # String (default)
        else:
            partition_value = str(partition_value)
            compare_value = str(compare_value)
        
        return partition_value, compare_value


@dataclass
class ColumnStatistics:
    """
    Min/max statistics for a column from Parquet metadata.
    Used for advanced pruning beyond directory-based filtering.
    """
    column_name: str
    min_value: Any
    max_value: Any
    null_count: int
    distinct_count: Optional[int] = None
    
    def can_satisfy_predicate(self, predicate: Predicate) -> bool:
        """
        Check if this column's value range could satisfy a predicate.
        
        Args:
            predicate: Predicate to check
            
        Returns:
            True if predicate could match (conservative)
        """
        if predicate.column != self.column_name:
            return True
        
        try:
            if predicate.operator == PredicateOperator.GT:
                return self.max_value > predicate.value
            elif predicate.operator == PredicateOperator.GTE:
                return self.max_value >= predicate.value
            elif predicate.operator == PredicateOperator.LT:
                return self.min_value < predicate.value
            elif predicate.operator == PredicateOperator.LTE:
                return self.min_value <= predicate.value
            elif predicate.operator == PredicateOperator.EQ:
                return self.min_value <= predicate.value <= self.max_value
            else:
                return True
        except:
            return True


@dataclass
class PredicateExtractionResult:
    """
    Result of extracting predicates from SQL WHERE clause.
    Used internally by partition pruner.
    """
    predicates: List[Predicate]
    table_name: str
    is_complex: bool
    
    def get_partition_predicates(self, partition_key: str) -> List[Predicate]:
        """Get predicates that apply to a specific partition column."""
        return [p for p in self.predicates if p.column == partition_key]
    
    def has_predicate_on(self, column: str) -> bool:
        """Check if any predicate references a column."""
        return any(p.column == column for p in self.predicates)


@dataclass
class PartitionInfo:
    """Information about a single partition."""
    path: str
    partition_key: str
    partition_value: str
    size_bytes: int
    file_count: int
    row_count: Optional[int] = None
    column_stats: Dict[str, ColumnStatistics] = field(default_factory=dict)
    
    @property
    def size_gb(self) -> float:
        """Size in gigabytes."""
        return self.size_bytes / (1024 ** 3)
    
    @property
    def size_mb(self) -> float:
        """Size in megabytes."""
        return self.size_bytes / (1024 ** 2)


@dataclass
class PruningResult:
    """Result of partition pruning operation."""
    partitions_to_scan: List[PartitionInfo]
    total_partitions: int
    total_size_bytes: int
    total_files: int
    predicates_applied: List[Predicate] = field(default_factory=list)
    pruning_time_sec: float = 0.0
    estimated_rows: Optional[int] = None
    
    @property
    def partitions_scanned(self) -> int:
        return len(self.partitions_to_scan)
    
    @property
    def pruning_ratio(self) -> float:
        if self.total_partitions == 0:
            return 0.0
        return 1.0 - (self.partitions_scanned / self.total_partitions)
    
    @property
    def size_gb(self) -> float:
        return self.total_size_bytes / (1024 ** 3)
    
    @property
    def speedup_estimate(self) -> float:
        if self.partitions_scanned == 0:
            return 1.0
        return self.total_partitions / self.partitions_scanned
    
    def summary(self) -> str:
        """Human-readable summary."""
        return (
            f"Partition Pruning Results:\n"
            f"  Partitions to scan: {self.partitions_scanned}/{self.total_partitions}\n"
            f"  Data to scan: {self.size_gb:.2f} GB\n"
            f"  Files to read: {self.total_files}\n"
            f"  Data skipped: {self.pruning_ratio*100:.1f}%\n"
            f"  Estimated speedup: {self.speedup_estimate:.1f}x\n"
            f"  Predicates applied: {len(self.predicates_applied)}"
        )


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
    partition_info: List[PartitionInfo] = field(default_factory=list)
    schema: Dict[str, str] = field(default_factory=dict)


@dataclass
class QueryResult:
    """Result of query execution."""
    data: Any
    backend_used: Backend
    execution_time_sec: float
    rows_processed: int
    partitions_scanned: int
    total_partitions: int
    from_cache: bool = False
    sql_optimized: Optional[str] = None
    pruning_result: Optional[PruningResult] = None
    actual_data_size_gb: float = 0.0


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