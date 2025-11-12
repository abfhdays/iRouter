#!/bin/bash
# Test CLI functionality

echo "=========================================="
echo "Intelligent Query Router - CLI Tests"
echo "=========================================="

# Make sure data exists
if [ ! -d "./data/sales" ]; then
    echo "Generating test data..."
    python scripts/generate_test_data.py
fi

echo ""
echo "Test 1: Simple Query"
echo "----------------------------------------"
irouter execute "SELECT * FROM sales WHERE date = '2024-11-01' LIMIT 5"

echo ""
echo ""
echo "Test 2: Aggregation Query"
echo "----------------------------------------"
irouter execute "SELECT region, COUNT(*) as cnt, SUM(amount) as total FROM sales WHERE date >= '2024-11-01' AND date <= '2024-11-05' GROUP BY region"

echo ""
echo ""
echo "Test 3: Explain Query"
echo "----------------------------------------"
irouter explain "SELECT customer_id, SUM(amount) FROM sales WHERE date >= '2024-11-01' AND date <= '2024-11-10' GROUP BY customer_id"

echo ""
echo ""
echo "Test 4: Force Backend"
echo "----------------------------------------"
irouter execute "SELECT COUNT(*) FROM sales" --backend duckdb

echo ""
echo ""
echo "Test 5: Cache Statistics"
echo "----------------------------------------"
irouter cache-stats

echo ""
echo ""
echo "Test 6: JSON Output"
echo "----------------------------------------"
irouter execute "SELECT region, SUM(amount) as total FROM sales WHERE date = '2024-11-01' GROUP BY region" --format json

echo ""
echo ""
echo "=========================================="
echo "All CLI tests complete!"
echo "=========================================="