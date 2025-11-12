"""DuckDB backend for query execution."""
import time
from typing import List
import pandas as pd
import duckdb

from irouter.backends.base import BaseBackend
from irouter.core.types import Backend, PruningResult


class DuckDBBackend(BaseBackend):
    """
    DuckDB backend for fast OLAP queries.
    
    Best for: < 10 GB data, single-machine workloads
    Features: Vectorized execution, fast aggregations, low overhead
    """
    
    def __init__(self):
        """Initialize DuckDB backend."""
        super().__init__()
        # Create in-memory DuckDB connection
        self.conn = duckdb.connect(":memory:")
    
    def execute(
        self,
        sql: str,
        pruning_result: PruningResult,
        table_name: str
    ) -> pd.DataFrame:
        """
        Execute SQL query using DuckDB.
        
        Args:
            sql: SQL query to execute
            pruning_result: Pruned partitions to read
            table_name: Table name in the query
            
        Returns:
            Query results as pandas DataFrame
            
        Example:
            >>> backend = DuckDBBackend()
            >>> result = backend.execute(
            ...     "SELECT * FROM sales WHERE amount > 100",
            ...     pruning_result,
            ...     "sales"
            ... )
            >>> print(len(result))
            1500
        """
        # Get list of Parquet files to read
        file_paths = self._get_file_paths(pruning_result)
        
        if not file_paths:
            # No files to read, return empty DataFrame
            return pd.DataFrame()
        
        # Create temporary view from Parquet files
        self._register_table(table_name, file_paths)
        
        # Execute query
        try:
            result_df = self.conn.execute(sql).df()
            return result_df
        except Exception as e:
            raise RuntimeError(f"DuckDB query execution failed: {e}")
    
    def _get_file_paths(self, pruning_result: PruningResult) -> List[str]:
        """
        Extract all Parquet file paths from pruned partitions.
        
        Args:
            pruning_result: Pruning result with partition info
            
        Returns:
            List of Parquet file paths
        """
        from pathlib import Path
        
        file_paths = []
        
        for partition in pruning_result.partitions_to_scan:
            partition_dir = Path(partition.path)
            
            # Get all Parquet files in this partition
            parquet_files = list(partition_dir.glob("*.parquet"))
            file_paths.extend([str(f) for f in parquet_files])
        
        return file_paths
    
    def _register_table(self, table_name: str, file_paths: List[str]):
        """
        Register Parquet files as a table in DuckDB.
        
        Args:
            table_name: Name to give the table
            file_paths: List of Parquet file paths to read
        """
        if len(file_paths) == 1:
            # Single file
            query = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{file_paths[0]}')"
        else:
            # Multiple files - use list syntax
            files_str = "[" + ", ".join(f"'{f}'" for f in file_paths) + "]"
            query = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet({files_str})"
        
        self.conn.execute(query)
    
    def get_backend_type(self) -> Backend:
        """Get backend type."""
        return Backend.DUCKDB
    
    def supports_feature(self, feature: str) -> bool:
        """
        Check if DuckDB supports a feature.
        
        Args:
            feature: Feature name
            
        Returns:
            True if supported
        """
        supported_features = {
            "window_functions": True,
            "cte": True,
            "recursive_cte": True,
            "lateral_join": True,
            "pivot": True,
            "unpivot": True,
        }
        
        return supported_features.get(feature, True)
    
    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close()