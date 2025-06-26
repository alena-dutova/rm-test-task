-- ============================================
-- Query A: Statistics by country
-- ============================================

WITH
-- Count all users grouped by country
total_users AS (
    SELECT
        country_code,
        COUNT(*) AS total_registered_users
    FROM users
    GROUP BY country_code
),

-- Users who made at least 1 deposit
users_with_deposit AS (
    SELECT DISTINCT
        b.user_id,
        u.country_code
    FROM balance b
	
	INNER JOIN users u ON u.user_id = b.user_id
    WHERE b.operation_type = 'deposit'
),

deposit_stats AS (
    SELECT
        u.country_code,
        AVG(b.operation_amount_usd) AS avg_deposit_amount
    FROM balance b
	
    INNER JOIN users u ON u.user_id = b.user_id
    WHERE b.operation_type = 'deposit'
    GROUP BY u.country_code
),

withdrawal_stats AS (
    SELECT
        u.country_code,
        AVG(b.operation_amount_usd) AS avg_withdrawal_amount
    FROM balance b
	
    INNER JOIN users u ON u.user_id = b.user_id
    WHERE b.operation_type = 'withdrawal'
    GROUP BY u.country_code
)

-- Final aggregation: join all metrics together
SELECT
    tu.country_code,
    tu.total_registered_users,
    COUNT(uwd.user_id) AS users_with_deposit,
    COALESCE(ROUND(ds.avg_deposit_amount::NUMERIC, 2), 0) AS avg_deposit_amount,
    COALESCE(ROUND(ws.avg_withdrawal_amount::NUMERIC, 2), 0) AS avg_withdrawal_amount
FROM total_users tu

LEFT JOIN users_with_deposit uwd ON tu.country_code = uwd.country_code
LEFT JOIN deposit_stats ds ON tu.country_code = ds.country_code
LEFT JOIN withdrawal_stats ws ON tu.country_code = ws.country_code

GROUP BY
    tu.country_code,
    tu.total_registered_users,
    ds.avg_deposit_amount,
    ws.avg_withdrawal_amount

ORDER BY
    tu.total_registered_users DESC;

-- ============================================
-- Query B: Active user. Find the user who has the highest profit in the entire history of trading operations and display for him
-- Supports ties (via RANK) and dynamic control of N(more usable)
-- ============================================

WITH top_users_ranked AS (
    -- Rank users by total profit (highest first)
    SELECT
        user_id,
        SUM(profit_usd) AS total_profit,
        RANK() OVER (ORDER BY SUM(profit_usd) DESC) AS profit_rank
    FROM orders
    GROUP BY user_id
),
top_users AS (
    -- Get top N users by profit
    SELECT *
    FROM top_users_ranked
    WHERE profit_rank <= 1  -- change this value to control "Top N"
),

top_user_orders AS (
    -- Extract all orders for top users
    SELECT o.*
    FROM orders o
    INNER JOIN top_users t ON o.user_id = t.user_id
),

profitability_stats AS (
    -- Compute per-user trade counts and number of profitable trades
    SELECT
        user_id,
        COUNT(*) AS total_transactions,
        COUNT(*) FILTER (WHERE profit_usd > 0) AS profitable_transactions
    FROM top_user_orders
    GROUP BY user_id
),

most_used_symbol AS (
    -- Most frequently traded instrument per user
    SELECT user_id, symbol
    FROM (
        SELECT
            user_id,
            symbol,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rn
        FROM top_user_orders
        GROUP BY user_id, symbol
    ) ranked
    WHERE rn = 1
),

most_profitable_symbol AS (
    -- Most profitable instrument per user
    SELECT user_id, symbol
    FROM (
        SELECT
            user_id,
            symbol,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY SUM(profit_usd) DESC) AS rn
        FROM top_user_orders
        GROUP BY user_id, symbol
    ) ranked
    WHERE rn = 1
),

most_unprofitable_symbol AS (
    -- Most unprofitable instrument per user
    SELECT user_id, symbol
    FROM (
        SELECT
            user_id,
            symbol,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY SUM(profit_usd) ASC) AS rn
        FROM top_user_orders
        GROUP BY user_id, symbol
    ) ranked
    WHERE rn = 1
)

-- Final result
SELECT
    t.user_id,
    u.country_code,
    ROUND(t.total_profit::NUMERIC, 2) AS final_profit,
    p.total_transactions,
    p.profitable_transactions,
    mus.symbol AS most_used_symbol,
    mps.symbol AS most_profitable_symbol,
    musym.symbol AS most_unprofitable_symbol

FROM top_users t

INNER JOIN users u ON u.user_id = t.user_id
INNER JOIN profitability_stats p ON p.user_id = t.user_id
INNER JOIN most_used_symbol mus ON mus.user_id = t.user_id
INNER JOIN most_profitable_symbol mps ON mps.user_id = t.user_id
INNER JOIN most_unprofitable_symbol musym ON musym.user_id = t.user_id;

-- ============================================
-- Query C: User funnel metrics
-- ============================================

WITH balance_30d AS (
    -- Aggregate deposits and withdrawals in the first 30 days post-registration
    SELECT
        b.user_id,
        SUM(CASE WHEN b.operation_type = 'deposit' THEN b.operation_amount_usd ELSE 0 END) AS deposit_30d,
        SUM(CASE WHEN b.operation_type = 'withdrawal' THEN b.operation_amount_usd ELSE 0 END) AS withdrawal_30d
    FROM balance b
	
    INNER JOIN users u 
		ON u.user_id = b.user_id
		
    WHERE b.operation_time BETWEEN u.registration_time AND u.registration_time + INTERVAL '30 days'
    GROUP BY b.user_id
),

orders_30d AS (
    -- Sum profit/loss during the first 30 days since registration
    SELECT
        o.user_id,
        SUM(o.profit_usd) AS profit_30d
    FROM orders o
	
    INNER JOIN users u 
		ON o.user_id = u.user_id
		
    WHERE o.close_time BETWEEN u.registration_time AND u.registration_time + INTERVAL '30 days'
    GROUP BY o.user_id
),

orders_all AS (
    -- Sum total profit/loss from all user in lifetime
    SELECT
        user_id,
        SUM(profit_usd) AS profit_total
    FROM orders
    GROUP BY user_id
)

-- Final selection combining all components of the user funnel
SELECT
    u.user_id,
    u.country_code,
    u.registration_time,
    ROUND(COALESCE(b.deposit_30d, 0)::NUMERIC, 2) AS deposit_first_30d,
    ROUND(COALESCE(b.withdrawal_30d, 0)::NUMERIC, 2) AS withdrawal_first_30d,
    ROUND(COALESCE(o30.profit_30d, 0)::NUMERIC, 2) AS profit_first_30d,
    ROUND(COALESCE(oall.profit_total, 0)::NUMERIC, 2) AS total_profit_lifetime

FROM users u

LEFT JOIN balance_30d b ON u.user_id = b.user_id
LEFT JOIN orders_30d o30 ON u.user_id = o30.user_id
LEFT JOIN orders_all oall ON u.user_id = oall.user_id
ORDER BY u.registration_time;
