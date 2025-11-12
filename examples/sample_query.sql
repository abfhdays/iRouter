-- Sample query for testing execute-file command
SELECT 
    region,
    customer_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_spent,
    AVG(amount) as avg_order_value,
    MIN(amount) as min_order,
    MAX(amount) as max_order
FROM sales
WHERE date >= '2024-11-01' AND date <= '2024-11-15'
GROUP BY region, customer_id
HAVING COUNT(*) >= 5
ORDER BY total_spent DESC
LIMIT 20;