"""Base backend interface."""
from abc import ABC, abstractmethod
from typing import Any, List, Optional
import pandas as pd

from irouter.core.types import Backend, PruningResult


class BaseBackend(ABC):
    """
    Abstract base class for query execution backends.
    
    All backends (DuckDB, Polars, Spark) implement this interface.
    """
    
    def __init__(self):
        """Initialize backend."""
        pass
    
    @abstractmethod
    def execute(
        self,
        sql: str,
        pruning_result: PruningResult,
        table_name: str
    ) -> pd.DataFrame:
        """
        Execute SQL query on pruned partitions.
        
        Args:
            sql: SQL query to execute
            pruning_result: Partition pruning result with file list
            table_name: Name of table being queried
            
        Returns:
            Query results as pandas DataFrame
            
        Raises:
            Exception: If query execution fails
        """
        pass
    
    @abstractmethod
    def get_backend_type(self) -> Backend:
        """
        Get backend type.
        
        Returns:
            Backend enum value
        """
        pass
    
    def supports_feature(self, feature: str) -> bool:
        """
        Check if backend supports a specific feature.
        
        Args:
            feature: Feature name (e.g., "window_functions", "cte")
            
        Returns:
            True if feature is supported
        """
        # Default: assume all features supported
        return True
    
    def close(self):
        """Clean up backend resources."""
        pass