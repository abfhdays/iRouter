# iRouter

**Cost-based SQL optimizer that automatically selects the fastest execution backend.**

Inspired and built with [SQLGlot](https://github.com/tobymao/sqlglot). I wanted to grow a deeper, intuitive understanding of how query engines work end-to-end, from query to execution. iRouter uses SQLGlot's core modules to replicate the SQL query process and attempts to optimize it further by intelligently parsing, partitioning, and executing, given the semantics and specific dialect of the query, as well as the scale of the DB executed against. iRouter achieves up to **~3x** speedup from SQLGlot. 

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Core Features

### SQL Parsing & AST Optimization
Uses SQLGlot to parse queries into an AST and run a subset of its built-in optimization passes — predicate pushdown, redundant expression removal, and basic type annotation. The optimizer works on the AST before execution, so backends receive a cleaner query than what was written. Parsing and optimization together take a few milliseconds for typical queries.
### Partition Pruning
Scans Hive-style `key=value` directory structures and filters down to the relevant Parquet files before any backend touches the data. It pulls predicates from the WHERE clause and matches them against partition paths, so a query like `WHERE date = '2024-11-01'` only opens files from that partition. 
### Backend Router
Given a query and an estimated data size, the router picks between DuckDB, Polars, and Spark using a set of hand-tuned heuristics. Small datasets go to DuckDB, medium to Polars, and large to Spark. Query complexity (joins, aggregations, window functions) adjusts the thresholds. 
### Orchestration
The engine wires together the full pipeline: parse, prune, extract features, select backend, execute, and cache the result. Total overhead before execution starts is under 20ms.


## CLI Tool
A thin command-line interface for running queries directly against local Parquet files without writing any Python. Supports passing SQL, inspecting the execution plan, and viewing basic stats on what the router decided and why.
**Files**: `src/irouter/cli/main.py`


**Commands**:
```bash
irouter execute "SELECT * FROM sales WHERE date = '2024-11-01'"
irouter explain "SELECT region, SUM(amount) FROM sales GROUP BY region"
irouter cache-stats
irouter benchmark
```

**CLI Usage**:
```bash
# Execute query
irouter execute "SELECT * FROM sales WHERE date = '2024-11-01'"

# Explain query plan
irouter explain "SELECT region, SUM(amount) FROM sales GROUP BY region"

# View cache stats
irouter cache-stats

# Benchmark backends
irouter benchmark
```

---

## End-to-End Performance

**Test Query**: Date-filtered aggregation (7 days of data)
```sql
SELECT region, COUNT(*), SUM(amount), AVG(amount)
FROM sales
WHERE date >= '2024-11-01' AND date <= '2024-11-07'
GROUP BY region
```

**Results**:
- **Partitions Pruned**: 23/30 (76.7% data skipped)
- **Backend Selected**: DuckDB (optimal for 0.18 GB)
- **Execution Time**: 0.089s

---



