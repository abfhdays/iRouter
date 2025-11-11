"""Partition pruning logic using SQLGlot."""
from typing import List, Optional
from pathlib import Path
import time

from irouter.sqlglot.parser import SQLParser
from irouter.core.types import (
    PartitionInfo,
    PruningResult,
    Predicate,
    PredicateExtractionResult,
)


class PartitionPruner:
    """Prunes partitions based on query predicates."""
    
    def __init__(self, data_path: str):
        """
        Initialize partition pruner.
        
        Args:
            data_path: Root path where data is stored
        """
        self.data_path = Path(data_path)
        self.parser = SQLParser()
    
    def prune(
        self, 
        table_name: str, 
        sql: str,
        schema: Optional[dict] = None
    ) -> PruningResult:
        """
        Return list of partitions that need to be scanned.
        
        Args:
            table_name: Name of table to prune
            sql: SQL query string
            schema: Optional schema for type inference
            
        Returns:
            PruningResult with partitions to scan and statistics
        """
        start_time = time.time()
        
        # Step 1: Parse SQL
        ast = self.parser.parse(sql)
        
        # Step 2: Optimize (pushdown predicates, simplify, etc.)
        optimized = self.parser.optimize(ast, schema=schema)
        
        # Step 3: Extract predicates
        extraction = self.parser.extract_predicates(optimized, table_name)
        
        # Step 4: Discover partitions
        table_path = self.data_path / table_name
        all_partitions = self._discover_partitions(table_path)
        
        # Step 5: Filter partitions based on predicates
        if not extraction.predicates or extraction.is_complex:
            # No predicates or too complex → scan everything
            matching_partitions = all_partitions
        else:
            matching_partitions = self._filter_partitions(
                all_partitions, 
                extraction
            )
        
        # Step 6: Calculate statistics
        total_size = sum(p.size_bytes for p in matching_partitions)
        total_files = sum(p.file_count for p in matching_partitions)
        
        pruning_time = time.time() - start_time
        
        return PruningResult(
            partitions_to_scan=matching_partitions,
            total_partitions=len(all_partitions),
            total_size_bytes=total_size,
            total_files=total_files,
            predicates_applied=extraction.predicates,
            pruning_time_sec=pruning_time
        )
    
    def _discover_partitions(self, table_path: Path) -> List[PartitionInfo]:
        """
        Discover partitions in Hive-style layout.
        
        Args:
            table_path: Path to table directory
            
        Returns:
            List of PartitionInfo objects
        """
        if not table_path.exists():
            raise FileNotFoundError(f"Table path not found: {table_path}")
        
        partitions = []
        
        # Look for Hive-style partitions (key=value directories)
        for partition_dir in table_path.iterdir():
            if not partition_dir.is_dir():
                continue
            
            # Parse partition name (e.g., "date=2024-11-01")
            if '=' not in partition_dir.name:
                continue
            
            partition_key, partition_value = partition_dir.name.split('=', 1)
            
            # Count Parquet files and calculate size
            parquet_files = list(partition_dir.glob("*.parquet"))
            
            if not parquet_files:
                continue
            
            total_size = sum(f.stat().st_size for f in parquet_files)
            
            partition = PartitionInfo(
                path=str(partition_dir),
                partition_key=partition_key,
                partition_value=partition_value,
                size_bytes=total_size,
                file_count=len(parquet_files)
            )
            
            partitions.append(partition)
        
        return partitions
    
    def _filter_partitions(
        self,
        partitions: List[PartitionInfo],
        extraction: PredicateExtractionResult
    ) -> List[PartitionInfo]:
        """
        Filter partitions using extracted predicates.
        
        Args:
            partitions: All available partitions
            extraction: Extracted predicates
            
        Returns:
            Filtered list of partitions to scan
        """
        matching = []
        
        for partition in partitions:
            # Get predicates that apply to this partition key
            relevant_predicates = extraction.get_partition_predicates(
                partition.partition_key
            )
            
            if not relevant_predicates:
                # No predicates for this partition key → include it
                matching.append(partition)
                continue
            
            # Check if partition satisfies ALL predicates (AND logic)
            if self._partition_matches(partition, relevant_predicates):
                matching.append(partition)
        
        return matching
    
    def _partition_matches(
        self,
        partition: PartitionInfo,
        predicates: List[Predicate]
    ) -> bool:
        """
        Check if partition satisfies all predicates.
        
        Args:
            partition: Partition to check
            predicates: List of predicates to evaluate
            
        Returns:
            True if partition matches all predicates
        """
        for predicate in predicates:
            if not predicate.evaluate(partition.partition_value):
                return False
        return True