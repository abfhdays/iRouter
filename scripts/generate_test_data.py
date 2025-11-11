"""Generate test data for partition pruning testing."""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa

def generate_sales_data(output_dir: str = "./data/sales"):
    """
    Generate partitioned sales data for testing.
    
    Creates data/sales/date=YYYY-MM-DD/ directories with Parquet files.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print("Generating test sales data...")
    print(f"Output directory: {output_path}")
    
    # Generate 30 days of data (one month)
    start_date = datetime(2024, 11, 1)
    num_days = 30
    
    # Rows per day (keep small for quick generation)
    rows_per_day = 1000
    
    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Create partition directory
        partition_dir = output_path / f"date={date_str}"
        partition_dir.mkdir(exist_ok=True)
        
        # Generate random sales data for this day
        data = {
            'customer_id': [f"CUST{np.random.randint(1, 1000):04d}" for _ in range(rows_per_day)],
            'amount': np.random.uniform(10, 5000, rows_per_day).round(2),
            'region': np.random.choice(['US', 'EU', 'APAC', 'LATAM'], rows_per_day),
            'product_id': [f"PROD{np.random.randint(1, 100):03d}" for _ in range(rows_per_day)],
            'quantity': np.random.randint(1, 10, rows_per_day),
        }
        
        df = pd.DataFrame(data)
        
        # Write as Parquet
        parquet_file = partition_dir / "data.parquet"
        df.to_parquet(parquet_file, index=False)
        
        # Calculate size
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        
        if (day_offset + 1) % 10 == 0:
            print(f"  Created {day_offset + 1}/{num_days} partitions...")
    
    # Print summary
    print("\n✓ Data generation complete!")
    print(f"\nSummary:")
    print(f"  Total partitions: {num_days}")
    print(f"  Date range: {start_date.strftime('%Y-%m-%d')} to {(start_date + timedelta(days=num_days-1)).strftime('%Y-%m-%d')}")
    print(f"  Rows per partition: {rows_per_day:,}")
    print(f"  Total rows: {rows_per_day * num_days:,}")
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in output_path.rglob("*.parquet"))
    print(f"  Total size: {total_size / (1024*1024):.2f} MB")
    
    print(f"\nPartitions created in: {output_path}")
    print("\nExample partition structure:")
    print("  data/sales/")
    print("    ├── date=2024-11-01/")
    print("    │   └── data.parquet")
    print("    ├── date=2024-11-02/")
    print("    │   └── data.parquet")
    print("    └── ...")
    
    print("\nNow run: python examples/test_partition_pruning.py")

def main():
    """Generate test data."""
    generate_sales_data()

if __name__ == "__main__":
    main()