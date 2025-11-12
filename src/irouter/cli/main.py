"""Command-line interface for Intelligent Query Router."""
import click
import sys
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax
import json

from irouter.engine import QueryEngine
from irouter.core.types import Backend

console = Console()


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """
    Intelligent Query Router - Cost-based SQL optimizer
    
    Automatically selects the fastest execution backend (DuckDB, Polars, Spark)
    based on query complexity and data size.
    """
    pass


@cli.command()
@click.argument('sql', type=str)
@click.option('--data-path', '-d', default='./data', help='Path to data directory')
@click.option('--backend', '-b', type=click.Choice(['duckdb', 'polars', 'spark'], case_sensitive=False),
              help='Force specific backend')
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv'], case_sensitive=False),
              default='table', help='Output format')
@click.option('--limit', '-l', type=int, help='Limit number of rows in output')
@click.option('--no-cache', is_flag=True, help='Bypass query cache')
@click.option('--schema', '-s', type=str, help='Table schema as JSON')
@click.option('--output', '-o', type=click.Path(), help='Output file path')
def execute(sql, data_path, backend, format, limit, no_cache, schema, output):
    """
    Execute SQL query and display results.
    
    Examples:
    
        # Simple query
        irouter execute "SELECT * FROM sales WHERE date = '2024-11-01'"
        
        # With backend selection
        irouter execute "SELECT COUNT(*) FROM sales" --backend duckdb
        
        # JSON output
        irouter execute "SELECT * FROM sales LIMIT 10" --format json
        
        # With schema
        irouter execute "SELECT * FROM sales" --schema '{"sales":{"date":"DATE"}}'
    """
    try:
        # Parse schema if provided
        schema_dict = None
        if schema:
            try:
                schema_dict = json.loads(schema)
            except json.JSONDecodeError:
                console.print("[red]Error: Invalid schema JSON[/red]")
                sys.exit(1)
        
        # Parse backend if provided
        backend_enum = None
        if backend:
            backend_enum = Backend[backend.upper()]
        
        # Create engine
        engine = QueryEngine(data_path=data_path)
        
        # Execute query
        console.print(f"[cyan]Executing query...[/cyan]")
        result = engine.execute(
            sql,
            schema=schema_dict,
            force_backend=backend_enum,
            bypass_cache=no_cache
        )
        
        # Apply limit if specified
        data = result.data
        if limit:
            data = data.head(limit)
        
        # Display results based on format
        if format == 'table':
            _display_table_result(result, data)
        elif format == 'json':
            _display_json_result(data, output)
        elif format == 'csv':
            _display_csv_result(data, output)
        
        # Show execution summary
        _display_execution_summary(result)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument('sql', type=str)
@click.option('--data-path', '-d', default='./data', help='Path to data directory')
@click.option('--schema', '-s', type=str, help='Table schema as JSON')
def explain(sql, data_path, schema):
    """
    Explain query execution plan without running.
    
    Shows:
    - Query complexity analysis
    - Partition pruning details
    - Backend selection reasoning
    - Cost estimates for all backends
    
    Examples:
    
        irouter explain "SELECT * FROM sales WHERE date = '2024-11-01'"
        
        irouter explain "SELECT region, SUM(amount) FROM sales GROUP BY region"
    """
    try:
        # Parse schema if provided
        schema_dict = None
        if schema:
            try:
                schema_dict = json.loads(schema)
            except json.JSONDecodeError:
                console.print("[red]Error: Invalid schema JSON[/red]")
                sys.exit(1)
        
        # Create engine
        engine = QueryEngine(data_path=data_path)
        
        # Get explanation
        explanation = engine.explain(sql, schema=schema_dict)
        
        console.print(explanation)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--data-path', '-d', default='./data', help='Path to data directory')
def cache_stats(data_path):
    """
    Display query cache statistics.
    
    Shows:
    - Cache size and capacity
    - Hit rate
    - Number of hits/misses
    - Evictions and expirations
    
    Example:
    
        irouter cache-stats
    """
    try:
        engine = QueryEngine(data_path=data_path, enable_cache=True)
        stats = engine.cache_stats()
        
        if not stats.get('enabled'):
            console.print("[yellow]Cache is disabled[/yellow]")
            return
        
        # Display stats
        console.print(Panel.fit(
            "[bold cyan]Query Cache Statistics[/bold cyan]",
            border_style="cyan"
        ))
        
        stats_table = Table(show_header=False, box=None)
        stats_table.add_column("Metric", style="cyan", width=20)
        stats_table.add_column("Value", style="green")
        
        stats_table.add_row("Cache Size", f"{stats['size']}/{stats['max_size']}")
        stats_table.add_row("Hit Rate", f"{stats['hit_rate']:.1%}")
        stats_table.add_row("Total Requests", str(stats['total_requests']))
        stats_table.add_row("Cache Hits", str(stats['hits']))
        stats_table.add_row("Cache Misses", str(stats['misses']))
        stats_table.add_row("Evictions", str(stats['evictions']))
        stats_table.add_row("Expirations", str(stats['expirations']))
        stats_table.add_row("Invalidations", str(stats['invalidations']))
        
        console.print(stats_table)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--data-path', '-d', default='./data', help='Path to data directory')
@click.confirmation_option(prompt='Are you sure you want to clear the cache?')
def cache_clear(data_path):
    """
    Clear all cached query results.
    
    Example:
    
        irouter cache-clear
    """
    try:
        engine = QueryEngine(data_path=data_path, enable_cache=True)
        engine.clear_cache()
        console.print("[green]✓ Cache cleared successfully[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument('sql_file', type=click.Path(exists=True))
@click.option('--data-path', '-d', default='./data', help='Path to data directory')
@click.option('--backend', '-b', type=click.Choice(['duckdb', 'polars', 'spark'], case_sensitive=False),
              help='Force specific backend')
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'csv'], case_sensitive=False),
              default='table', help='Output format')
@click.option('--no-cache', is_flag=True, help='Bypass query cache')
def execute_file(sql_file, data_path, backend, format, no_cache):
    """
    Execute SQL query from file.
    
    Example:
    
        irouter execute-file query.sql
        
        irouter execute-file query.sql --format json > results.json
    """
    try:
        # Read SQL from file
        with open(sql_file, 'r') as f:
            sql = f.read()
        
        console.print(f"[cyan]Reading query from: {sql_file}[/cyan]")
        console.print(Syntax(sql, "sql", theme="monokai", line_numbers=False))
        
        # Parse backend if provided
        backend_enum = None
        if backend:
            backend_enum = Backend[backend.upper()]
        
        # Create engine and execute
        engine = QueryEngine(data_path=data_path)
        result = engine.execute(
            sql,
            force_backend=backend_enum,
            bypass_cache=no_cache
        )
        
        # Display results
        if format == 'table':
            _display_table_result(result, result.data)
        elif format == 'json':
            _display_json_result(result.data, None)
        elif format == 'csv':
            _display_csv_result(result.data, None)
        
        _display_execution_summary(result)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--data-path', '-d', default='./data', help='Path to data directory')
def benchmark(data_path):
    """
    Run benchmark comparing all backends.
    
    Executes the same query on DuckDB, Polars, and Spark
    to compare performance.
    
    Example:
    
        irouter benchmark
    """
    try:
        console.print(Panel.fit(
            "[bold cyan]Backend Performance Benchmark[/bold cyan]",
            border_style="cyan"
        ))
        
        engine = QueryEngine(data_path=data_path)
        
        sql = """
            SELECT 
                region,
                COUNT(*) as transactions,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM sales
            WHERE date >= '2024-11-01' AND date <= '2024-11-15'
            GROUP BY region
            ORDER BY total_amount DESC
        """
        
        console.print("\n[cyan]Query:[/cyan]")
        console.print(Syntax(sql, "sql", theme="monokai", line_numbers=False))
        
        schema = {"sales": {
            "date": "DATE",
            "region": "VARCHAR",
            "amount": "DECIMAL"
        }}
        
        results = {}
        
        console.print("\n[cyan]Executing on each backend...[/cyan]")
        
        for backend in [Backend.DUCKDB, Backend.POLARS, Backend.SPARK]:
            try:
                console.print(f"  {backend.value}...", end="")
                result = engine.execute(sql, schema=schema, force_backend=backend)
                results[backend] = result.execution_time_sec
                console.print(f" {result.execution_time_sec:.3f}s")
            except Exception as e:
                console.print(f" [red]FAILED[/red]")
                results[backend] = None
        
        # Display comparison
        _display_benchmark_results(results)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


# Helper functions

def _display_table_result(result, data):
    """Display query result as formatted table."""
    console.print("\n[bold cyan]Query Results:[/bold cyan]")
    
    if len(data) == 0:
        console.print("[yellow]No rows returned[/yellow]")
        return
    
    # Create table
    table = Table(show_header=True, header_style="bold cyan")
    
    # Add columns
    for col in data.columns:
        table.add_column(str(col))
    
    # Add rows (limit to 100 for display)
    display_limit = min(len(data), 100)
    for _, row in data.head(display_limit).iterrows():
        table.add_row(*[str(v) for v in row])
    
    if len(data) > display_limit:
        table.add_row(*["..." for _ in data.columns])
    
    console.print(table)
    
    if len(data) > display_limit:
        console.print(f"\n[yellow]Showing {display_limit} of {len(data)} rows[/yellow]")


def _display_json_result(data, output_file):
    """Display query result as JSON."""
    json_str = data.to_json(orient='records', indent=2)
    
    if output_file:
        with open(output_file, 'w') as f:
            f.write(json_str)
        console.print(f"[green]Results written to: {output_file}[/green]")
    else:
        console.print(json_str)


def _display_csv_result(data, output_file):
    """Display query result as CSV."""
    csv_str = data.to_csv(index=False)
    
    if output_file:
        with open(output_file, 'w') as f:
            f.write(csv_str)
        console.print(f"[green]Results written to: {output_file}[/green]")
    else:
        console.print(csv_str)


def _display_execution_summary(result):
    """Display execution summary."""
    console.print("\n[bold cyan]Execution Summary:[/bold cyan]")
    
    summary_table = Table(show_header=False, box=None)
    summary_table.add_column("Metric", style="cyan", width=20)
    summary_table.add_column("Value", style="green")
    
    summary_table.add_row("Backend", result.backend_used.value.upper())
    summary_table.add_row("Execution Time", f"{result.execution_time_sec:.3f}s")
    summary_table.add_row("Rows Processed", f"{result.rows_processed:,}")
    summary_table.add_row("Partitions Scanned", f"{result.partitions_scanned}/{result.total_partitions}")
    summary_table.add_row("Data Scanned", f"{result.actual_data_size_gb:.2f} GB")
    summary_table.add_row("From Cache", "✓" if result.from_cache else "✗")
    
    if result.pruning_result:
        summary_table.add_row("Pruning Speedup", f"{result.pruning_result.speedup_estimate:.1f}x")
    
    console.print(summary_table)


def _display_benchmark_results(results):
    """Display benchmark comparison."""
    console.print("\n[bold cyan]Benchmark Results:[/bold cyan]")
    
    # Find fastest
    valid_results = {k: v for k, v in results.items() if v is not None}
    if not valid_results:
        console.print("[red]All backends failed[/red]")
        return
    
    fastest_time = min(valid_results.values())
    fastest_backend = min(valid_results.keys(), key=lambda k: valid_results[k])
    
    # Create table
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Backend")
    table.add_column("Execution Time", justify="right")
    table.add_column("Relative Speed")
    table.add_column("Winner")
    
    for backend in [Backend.DUCKDB, Backend.POLARS, Backend.SPARK]:
        time = results.get(backend)
        if time is not None:
            relative = f"{time/fastest_time:.2f}x"
            winner = "⭐" if backend == fastest_backend else ""
            table.add_row(
                backend.value,
                f"{time:.3f}s",
                relative,
                winner
            )
        else:
            table.add_row(backend.value, "FAILED", "-", "")
    
    console.print(table)
    console.print(f"\n[bold green]Winner: {fastest_backend.value.upper()} ({fastest_time:.3f}s)[/bold green]")


if __name__ == '__main__':
    cli()