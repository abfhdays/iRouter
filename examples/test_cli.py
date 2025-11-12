"""Test CLI functionality."""
import subprocess
import json
from rich.console import Console

console = Console()


def run_command(cmd):
    """Run CLI command and return output."""
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True
    )
    return result.stdout, result.stderr, result.returncode


def test_simple_execute():
    """Test simple query execution."""
    console.print("\n[bold cyan]Test 1: Simple Execute[/bold cyan]")
    
    cmd = 'irouter execute "SELECT COUNT(*) FROM sales WHERE date = \'2024-11-01\'"'
    stdout, stderr, code = run_command(cmd)
    
    if code == 0:
        console.print("[green]✓ Command succeeded[/green]")
        console.print(stdout)
    else:
        console.print(f"[red]✗ Command failed: {stderr}[/red]")


def test_json_output():
    """Test JSON output format."""
    console.print("\n[bold cyan]Test 2: JSON Output[/bold cyan]")
    
    cmd = 'irouter execute "SELECT region, COUNT(*) as cnt FROM sales WHERE date = \'2024-11-01\' GROUP BY region" --format json'
    stdout, stderr, code = run_command(cmd)
    
    if code == 0:
        try:
            # Try to parse JSON
            json.loads(stdout)
            console.print("[green]✓ Valid JSON output[/green]")
        except json.JSONDecodeError:
            console.print("[yellow]⚠ Output is not valid JSON[/yellow]")
    else:
        console.print(f"[red]✗ Command failed: {stderr}[/red]")


def test_explain():
    """Test explain command."""
    console.print("\n[bold cyan]Test 3: Explain[/bold cyan]")
    
    cmd = 'irouter explain "SELECT * FROM sales WHERE date >= \'2024-11-01\' AND date <= \'2024-11-05\'"'
    stdout, stderr, code = run_command(cmd)
    
    if code == 0:
        console.print("[green]✓ Explain succeeded[/green]")
        if "QUERY EXECUTION PLAN" in stdout:
            console.print("[green]✓ Contains execution plan[/green]")
        else:
            console.print("[yellow]⚠ Missing execution plan[/yellow]")
    else:
        console.print(f"[red]✗ Command failed: {stderr}[/red]")


def test_cache_stats():
    """Test cache statistics."""
    console.print("\n[bold cyan]Test 4: Cache Stats[/bold cyan]")
    
    cmd = 'irouter cache-stats'
    stdout, stderr, code = run_command(cmd)
    
    if code == 0:
        console.print("[green]✓ Cache stats retrieved[/green]")
        if "Cache Size" in stdout or "Hit Rate" in stdout:
            console.print("[green]✓ Contains cache information[/green]")
    else:
        console.print(f"[red]✗ Command failed: {stderr}[/red]")


def test_backend_selection():
    """Test forcing specific backend."""
    console.print("\n[bold cyan]Test 5: Backend Selection[/bold cyan]")
    
    for backend in ['duckdb', 'polars', 'spark']:
        cmd = f'irouter execute "SELECT COUNT(*) FROM sales LIMIT 1" --backend {backend}'
        stdout, stderr, code = run_command(cmd)
        
        if code == 0:
            console.print(f"[green]✓ {backend.upper()} backend worked[/green]")
        else:
            console.print(f"[yellow]⚠ {backend.upper()} backend failed[/yellow]")


def test_help():
    """Test help commands."""
    console.print("\n[bold cyan]Test 6: Help Commands[/bold cyan]")
    
    commands = [
        'irouter --help',
        'irouter execute --help',
        'irouter explain --help',
        'irouter cache-stats --help'
    ]
    
    for cmd in commands:
        stdout, stderr, code = run_command(cmd)
        if code == 0:
            console.print(f"[green]✓ {cmd}[/green]")
        else:
            console.print(f"[red]✗ {cmd}[/red]")


def main():
    """Run all CLI tests."""
    console.print("[bold cyan]=" * 60 + "[/bold cyan]")
    console.print("[bold cyan]CLI Functionality Tests[/bold cyan]")
    console.print("[bold cyan]=" * 60 + "[/bold cyan]")
    
    test_simple_execute()
    test_json_output()
    test_explain()
    test_cache_stats()
    test_backend_selection()
    test_help()
    
    console.print("\n[bold green]All CLI tests complete![/bold green]")


if __name__ == "__main__":
    main()