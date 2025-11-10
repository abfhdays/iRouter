"""
SQL Query
    ↓
WHERE clause extracted
    ↓
Individual predicates (date >= '2024-11-01', region = 'US')
    ↓
Partition directories (/sales/date=2024-11-01/, /sales/date=2024-11-02/)
    ↓
Match predicates against partitions
    ↓
List of partitions to scan + statistics
    ↓
Backend execution

"""