#!/bin/bash

# In your shell script, add before running Python:
export HADOOP_HOME=/dev/null


# iRouter Demo Script for asciinema
# This script demonstrates the key features of the Intelligent SQL Query Router

# Colors for better visualization
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Add small pauses between commands for readability
PAUSE=2

clear

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  iRouter: Intelligent SQL Query Router Demo${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
sleep $PAUSE

# Demo 1: Small dataset - DuckDB selection
echo -e "${YELLOW}[1/5] Simple query on small dataset → DuckDB selected${NC}"
echo ""
irouter execute "SELECT * FROM sales WHERE date = '2024-11-01' LIMIT 10"
echo ""
sleep $PAUSE

# Demo 2: Medium aggregation - Show execution plan
echo -e "${YELLOW}[2/5] Regional aggregation → Showing execution plan${NC}"
echo ""
irouter explain "SELECT region, COUNT(*), SUM(amount) FROM sales WHERE date >= '2024-11-01' GROUP BY region"
echo ""
sleep $PAUSE

# Demo 3: Partition pruning demonstration
echo -e "${YELLOW}[3/5] Date-filtered query → Partition pruning in action${NC}"
echo ""
irouter execute "SELECT region, SUM(amount) FROM sales WHERE date >= '2024-11-01' AND date <= '2024-11-07' GROUP BY region"
echo ""
sleep $PAUSE

# Demo 4: Cache demonstration - run same query twice
echo -e "${YELLOW}[4/5] Running same query again → Cache hit!${NC}"
echo ""
irouter execute "SELECT region, SUM(amount) FROM sales WHERE date >= '2024-11-01' AND date <= '2024-11-07' GROUP BY region"
echo ""
sleep $PAUSE

# Demo 5: Cache statistics
echo -e "${YELLOW}[5/5] Cache performance statistics${NC}"
echo ""
irouter cache-stats
echo ""
sleep 1

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Demo complete! 🚀${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
