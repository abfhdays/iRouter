"""Test partition pruning with SQLGlot integration."""
from irouter.optimizer.partition_pruning import PartitionPruner
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

def main():
    """Test partition pruning end-to-end."""
    
    console.print(Panel.fit(
        "[bold cyan]Partition Pruning Test[/bold cyan]",
        border_style="cyan"
    ))
    
    # Initialize pruner
    pruner = PartitionPruner(data_path="./data")
    
    # Test query
    sql = """
        SELECT 
            customer_id,
            SUM(amount) as total
        FROM sales
        WHERE date >= '2024-11-01'
            AND date <= '2024-11-07'
        GROUP BY customer_id
    """
    
    console.print("\n[bold cyan]SQL Query:[/bold cyan]")
    console.print(sql)
    
    # Define schema
    schema = {
        "sales": {
            "customer_id": "VARCHAR",
            "amount": "DECIMAL",
            "date": "DATE",
            "region": "VARCHAR"
        }
    }
    
    # Run pruning
    console.print("\n[bold cyan]Running partition pruning...[/bold cyan]")
    
    try:
        result = pruner.prune(
            table_name="sales",
            sql=sql,
            schema=schema
        )
        
        # Display results
        console.print(f"\n[bold green]✓ Pruning complete in {result.pruning_time_sec:.3f}s[/bold green]\n")
        
        # Show summary
        console.print(Panel(result.summary(), title="Summary", border_style="green"))
        
        # Show predicates
        if result.predicates_applied:
            console.print("\n[bold cyan]Predicates Extracted:[/bold cyan]")
            predicate_table = Table(show_header=True, header_style="bold cyan")
            predicate_table.add_column("Column")
            predicate_table.add_column("Operator")
            predicate_table.add_column("Value")
            predicate_table.add_column("Type")
            
            for pred in result.predicates_applied:
                predicate_table.add_row(
                    pred.column,
                    pred.operator.value,
                    str(pred.value),
                    pred.sql_type or "UNKNOWN"
                )
            
            console.print(predicate_table)
        
        # Show partitions to scan
        if result.partitions_to_scan:
            console.print("\n[bold cyan]Partitions to Scan:[/bold cyan]")
            partition_table = Table(show_header=True, header_style="bold cyan")
            partition_table.add_column("Partition Key")
            partition_table.add_column("Partition Value")
            partition_table.add_column("Size (MB)")
            partition_table.add_column("Files")
            
            for partition in result.partitions_to_scan[:10]:
                partition_table.add_row(
                    partition.partition_key,
                    partition.partition_value,
                    f"{partition.size_mb:.2f}",
                    str(partition.file_count)
                )
            
            if len(result.partitions_to_scan) > 10:
                partition_table.add_row("...", "...", "...", "...")
            
            console.print(partition_table)
        else:
            console.print("\n[yellow]No partitions to scan (table might not exist)[/yellow]")
            
    except FileNotFoundError as e:
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        console.print("\n[yellow]Note: Create test data first using generate_test_data.py[/yellow]")
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()