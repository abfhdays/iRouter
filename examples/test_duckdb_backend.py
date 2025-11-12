"""Test DuckDB backend execution."""
from irouter.optimizer.partition_pruning import PartitionPruner
from irouter.backends.duckdb_backend import DuckDBBackend
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import time

console = Console()


def test_simple_query():
    """Test simple SELECT query."""
    console.print("\n[bold cyan]Test 1: Simple SELECT Query[/bold cyan]")
    
    # Setup
    pruner = PartitionPruner(data_path="./data")
    backend = DuckDBBackend()
    
    # SQL query
    sql = """
        SELECT * FROM sales
        WHERE date = '2024-11-01'
    """
    
    console.print(f"SQL: {sql}")
    
    # Prune partitions
    pruning_result = pruner.prune(
        table_name="sales",
        sql=sql,
        schema={"sales": {"date": "DATE"}}
    )
    
    console.print(f"Partitions to scan: {pruning_result.partitions_scanned}/{pruning_result.total_partitions}")
    
    # Execute
    start_time = time.time()
    result = backend.execute(sql, pruning_result, "sales")
    execution_time = time.time() - start_time
    
    console.print(f"\n[bold green]✓ Query executed successfully![/bold green]")
    console.print(f"Rows returned: {len(result):,}")
    console.print(f"Execution time: {execution_time:.3f}s")
    console.print(f"Columns: {list(result.columns)}")
    
    # Show sample data
    if len(result) > 0:
        console.print("\n[bold cyan]Sample Data (first 5 rows):[/bold cyan]")
        sample_table = Table(show_header=True, header_style="bold cyan")
        
        for col in result.columns:
            sample_table.add_column(col)
        
        for _, row in result.head(5).iterrows():
            sample_table.add_row(*[str(v) for v in row])
        
        console.print(sample_table)


def test_aggregation_query():
    """Test aggregation query."""
    console.print("\n[bold cyan]Test 2: Aggregation Query[/bold cyan]")
    
    pruner = PartitionPruner(data_path="./data")
    backend = DuckDBBackend()
    
    sql = """
        SELECT 
            region,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            MIN(amount) as min_amount,
            MAX(amount) as max_amount
        FROM sales
        WHERE date >= '2024-11-01' AND date <= '2024-11-07'
        GROUP BY region
        ORDER BY total_amount DESC
    """
    
    console.print(f"SQL: Aggregation by region for 7 days")
    
    # Prune partitions
    pruning_result = pruner.prune(
        table_name="sales",
        sql=sql,
        schema={"sales": {"date": "DATE", "region": "VARCHAR", "amount": "DECIMAL"}}
    )
    
    console.print(f"Partitions to scan: {pruning_result.partitions_scanned}/{pruning_result.total_partitions}")
    console.print(f"Data size: {pruning_result.size_gb:.2f} GB")
    
    # Execute
    start_time = time.time()
    result = backend.execute(sql, pruning_result, "sales")
    execution_time = time.time() - start_time
    
    console.print(f"\n[bold green]✓ Aggregation executed![/bold green]")
    console.print(f"Execution time: {execution_time:.3f}s")
    
    # Show results
    console.print("\n[bold cyan]Aggregation Results:[/bold cyan]")
    agg_table = Table(show_header=True, header_style="bold cyan")
    agg_table.add_column("Region")
    agg_table.add_column("Transactions", justify="right")
    agg_table.add_column("Total Amount", justify="right")
    agg_table.add_column("Avg Amount", justify="right")
    agg_table.add_column("Min", justify="right")
    agg_table.add_column("Max", justify="right")
    
    for _, row in result.iterrows():
        agg_table.add_row(
            row['region'],
            f"{row['transaction_count']:,}",
            f"${row['total_amount']:,.2f}",
            f"${row['avg_amount']:.2f}",
            f"${row['min_amount']:.2f}",
            f"${row['max_amount']:.2f}"
        )
    
    console.print(agg_table)


def test_date_range_query():
    """Test query with date range."""
    console.print("\n[bold cyan]Test 3: Date Range Query[/bold cyan]")
    
    pruner = PartitionPruner(data_path="./data")
    backend = DuckDBBackend()
    
    sql = """
        SELECT 
            customer_id,
            COUNT(*) as purchase_count,
            SUM(amount) as total_spent
        FROM sales
        WHERE date >= '2024-11-01' AND date <= '2024-11-10'
        GROUP BY customer_id
        HAVING SUM(amount) > 10000
        ORDER BY total_spent DESC
        LIMIT 10
    """
    
    console.print(f"SQL: Top 10 customers by spend (first 10 days)")
    
    # Prune partitions
    pruning_result = pruner.prune(
        table_name="sales",
        sql=sql,
        schema={"sales": {"date": "DATE", "customer_id": "VARCHAR", "amount": "DECIMAL"}}
    )
    
    console.print(f"Partitions to scan: {pruning_result.partitions_scanned}/{pruning_result.total_partitions}")
    console.print(f"Speedup estimate: {pruning_result.speedup_estimate:.1f}x")
    
    # Execute
    start_time = time.time()
    result = backend.execute(sql, pruning_result, "sales")
    execution_time = time.time() - start_time
    
    console.print(f"\n[bold green]✓ Query executed![/bold green]")
    console.print(f"Top customers found: {len(result)}")
    console.print(f"Execution time: {execution_time:.3f}s")
    
    # Show results
    if len(result) > 0:
        console.print("\n[bold cyan]Top Customers:[/bold cyan]")
        top_table = Table(show_header=True, header_style="bold cyan")
        top_table.add_column("Rank")
        top_table.add_column("Customer ID")
        top_table.add_column("Purchases", justify="right")
        top_table.add_column("Total Spent", justify="right")
        
        for idx, (_, row) in enumerate(result.iterrows(), 1):
            top_table.add_row(
                str(idx),
                row['customer_id'],
                str(row['purchase_count']),
                f"${row['total_spent']:,.2f}"
            )
        
        console.print(top_table)


def test_performance_comparison():
    """Test performance with and without partition pruning."""
    console.print("\n[bold cyan]Test 4: Performance Comparison[/bold cyan]")
    
    pruner = PartitionPruner(data_path="./data")
    backend = DuckDBBackend()
    
    # Query that benefits from pruning
    sql_filtered = """
        SELECT COUNT(*), SUM(amount)
        FROM sales
        WHERE date = '2024-11-15'
    """
    
    console.print("Comparing: Single day query (date = '2024-11-15')")
    
    # With pruning
    pruning_result = pruner.prune(
        table_name="sales",
        sql=sql_filtered,
        schema={"sales": {"date": "DATE"}}
    )
    
    start_time = time.time()
    result_pruned = backend.execute(sql_filtered, pruning_result, "sales")
    time_pruned = time.time() - start_time
    
    console.print(f"\n[bold green]With Partition Pruning:[/bold green]")
    console.print(f"  Partitions scanned: {pruning_result.partitions_scanned}/{pruning_result.total_partitions}")
    console.print(f"  Execution time: {time_pruned:.3f}s")
    console.print(f"  Theoretical speedup: {pruning_result.speedup_estimate:.1f}x")


def main():
    """Run all DuckDB backend tests."""
    console.print(Panel.fit(
        "[bold cyan]DuckDB Backend Tests[/bold cyan]",
        border_style="cyan"
    ))
    
    try:
        test_simple_query()
        test_aggregation_query()
        test_date_range_query()
        test_performance_comparison()
        
        console.print("\n[bold green]✓ All DuckDB backend tests passed![/bold green]")
        
    except FileNotFoundError as e:
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        console.print("[yellow]Run: python scripts/generate_test_data.py[/yellow]")
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()