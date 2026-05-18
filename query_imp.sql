-- Analytics Query
-- Database: analytics
-- Tables: raw_orders, product_analytics, hourly_analytics,department_analytics

-- A. Product Analytics
-- A1. Top 10 most ordered products
-- Visualisasi: Row chart
SELECT
    product_name,
    department,
    total_orders,
    reorder_count,
    reorder_rate
FROM analytics.product_analytics
ORDER BY total_orders DESC
LIMIT 10;

-- A2. Top 10 products with the highest reorder rate (min 5 orders)
-- Visualisasi: Row chart
SELECT
    product_name,
    department,
    total_orders,
    reorder_count,
    reorder_rate
FROM analytics.product_analytics
WHERE
    total_orders >= 5
ORDER BY reorder_rate DESC
LIMIT 10;

-- A3. Products with Lowest Reorder Rate
-- Visualisasi: Row chart
SELECT
    product_name,
    department,
    total_orders,
    reorder_count,
    reorder_rate
FROM analytics.product_analytics
ORDER BY reorder_rate ASC
LIMIT 10;

-- A4. Unique Products & Total Orders per Department
-- Visualisasi: Bar chart (departments on x-axis, total orders on y-axis)
SELECT
    department,
    count(product_name) AS total_products,
    sum(total_orders) AS total_orders_dept
FROM analytics.product_analytics
GROUP BY
    department
ORDER BY total_orders_dept DESC;

-- A5. Average Reorder Rate per Department
-- Visualisasi: Bar chart (departments on x-axis, average reorder rate on y-axis)
SELECT
    department,
    round(avg(reorder_rate), 4) AS avg_reorder_rate,
    sum(total_orders) AS total_orders
FROM analytics.product_analytics
GROUP BY
    department
ORDER BY avg_reorder_rate DESC;

-- A6. Top 5 produk per department (window function)
-- Visualisasi: Table
SELECT
    department,
    product_name,
    total_orders,
    reorder_rate,
    rank() OVER (
        PARTITION BY
            department
        ORDER BY total_orders DESC
    ) AS rank_in_dept
FROM analytics.product_analytics
WHERE
    rank_in_dept <= 5
ORDER BY department, rank_in_dept;

-- A7. Evergreen Products: High Orders & High Loyalty
--Visualisasi: Table
SELECT
    product_name,
    department,
    total_orders,
    reorder_rate,
    round(
        total_orders * reorder_rate,
        2
    ) AS loyalty_score
FROM analytics.product_analytics
WHERE
    total_orders >= 30
ORDER BY loyalty_score DESC
LIMIT 15;

-- A8. Reorder Rate Distribution by Bucket
--Visualisasi: Pie Chart
SELECT
    CASE
        WHEN reorder_rate >= 0.8 THEN 'High (>=80%)'
        WHEN reorder_rate >= 0.5 THEN 'Medium (50-79%)'
        WHEN reorder_rate >= 0.2 THEN 'Low (20-49%)'
        ELSE 'Very Low (<20%)'
    END AS reorder_bucket,
    count(*) AS product_count,
    sum(total_orders) AS total_orders
FROM analytics.product_analytics
GROUP BY
    reorder_bucket
ORDER BY total_orders DESC;

-- A9. Total Orders vs Reorders by Department
-- Visualisasi: Combo Bar Chart
SELECT
    department,
    sum(total_orders) AS total_orders,
    sum(reorder_count) AS total_reorders,
    round(
        sum(reorder_count) / sum(total_orders),
        4
    ) AS dept_reorder_rate
FROM analytics.product_analytics
GROUP BY
    department
ORDER BY dept_reorder_rate DESC;

-- A10. Produk first - time buyer dominant
-- Visualisasi: Row Chart
SELECT
    product_name,
    department,
    total_orders,
    reorder_rate,
    (total_orders - reorder_count) AS new_buyer_orders
FROM analytics.product_analytics
WHERE
    reorder_rate < 0.3
    AND total_orders >= 10
ORDER BY new_buyer_orders DESC
LIMIT 15;

-- B. Hourly Analytics
-- B1. Distribusi Order per Hour
SELECT
    order_hour_of_day,
    total_orders,
    round(
        total_orders * 100.0 / sum(total_orders) OVER (),
        2
    ) AS pct_of_day
FROM analytics.hourly_analytics
ORDER BY order_hour_of_day;

-- B2. Time Segmentation (Morning / Afternoon / Evening / Night)
SELECT
    CASE
        WHEN order_hour_of_day BETWEEN 5 AND 11  THEN 'Morning (05-11)'
        WHEN order_hour_of_day BETWEEN 12 AND 17  THEN 'Afternoon (12-17)'
        WHEN order_hour_of_day BETWEEN 18 AND 21  THEN 'Evening (18-21)'
        ELSE 'Night (22-04)'
    END AS time_segment,
    sum(total_orders) AS total_orders,
    round(
        sum(total_orders) * 100.0 / sum(sum(total_orders)) OVER (),
        2
    ) AS pct
FROM analytics.hourly_analytics
GROUP BY
    time_segment
ORDER BY total_orders DESC;

-- B3. Running total order through the day
SELECT
    order_hour_of_day,
    total_orders,
    sum(total_orders) OVER (
        ORDER BY order_hour_of_day
    ) AS cumulative_orders
FROM analytics.hourly_analytics
ORDER BY order_hour_of_day;

-- B4. Hourly order performance vs daily average
SELECT
    order_hour_of_day,
    total_orders,
    round(avg(total_orders) OVER (), 2) AS daily_avg,
    CASE
        WHEN total_orders > avg(total_orders) OVER () THEN 'Above Average'
        ELSE 'Below Average'
    END AS vs_avg
FROM analytics.hourly_analytics
ORDER BY order_hour_of_day;

-- C. Department Analytics
-- C1. Department contribution to total transactions
SELECT
    department,
    total_orders,
    round(
        total_orders * 100.0 / sum(total_orders) OVER (),
        2
    ) AS market_share_pct
FROM analytics.department_analytics
ORDER BY total_orders DESC;

-- C2. Concentration Risk Analysis
SELECT
    COUNT(*) AS total_departments,
    SUM(
        CASE
            WHEN total_orders >= 50 THEN 1
            ELSE 0
        END
    ) AS major_departments,
    ROUND(
        SUM(
            CASE
                WHEN total_orders >= 50 THEN total_orders
                ELSE 0
            END
        ) * 100.0 / SUM(total_orders),
        2
    ) AS pct_orders_from_major_departments
FROM analytics.department_analytics;

-- C3. Anomaly Detection (Z-Score)
WITH
    stats AS (
        SELECT AVG(total_orders) AS avg_orders, STDDEV_POP(total_orders) AS std_orders
        FROM analytics.department_analytics
    )
SELECT
    d.department,
    d.total_orders,
    ROUND(
        (d.total_orders - s.avg_orders) / NULLIF(s.std_orders, 0),
        2
    ) AS z_score,
    CASE
        WHEN ABS(
            (d.total_orders - s.avg_orders) / NULLIF(s.std_orders, 0)
        ) >= 2 THEN 'Anomaly'
        ELSE 'Normal'
    END AS anomaly_status
FROM analytics.department_analytics d
    CROSS JOIN stats s
ORDER BY z_score DESC;

-- C4. Department with total orders above and below average
SELECT
    department,
    total_orders,
    ROUND(avg_orders, 2) AS avg_orders,
    CASE
        WHEN total_orders > avg_orders THEN 'Above Average'
        ELSE 'Below Average'
    END AS performance
FROM (
        SELECT
            department, total_orders, AVG(total_orders) OVER () AS avg_orders
        FROM analytics.department_analytics
    )
ORDER BY total_orders DESC;

-- C5. Total Orders Gap to next department
SELECT
    department,
    total_orders,
    RANK() OVER (
        ORDER BY total_orders DESC
    ) AS dept_rank,
    total_orders - LEAD(total_orders) OVER (
        ORDER BY total_orders DESC
    ) AS gap_to_next
FROM analytics.department_analytics
ORDER BY total_orders DESC;

-- D. Raw Orders
-- D1. All transactions
SELECT
    count(*) AS total_rows,
    count(DISTINCT order_id) AS unique_orders,
    count(DISTINCT user_id) AS unique_users,
    count(DISTINCT product_id) AS unique_products
FROM analytics.raw_orders;

-- D2. Average item per order
SELECT
    round(
        count(*) / count(DISTINCT order_id),
        2
    ) AS avg_items_per_order,
    max(add_to_cart_order) AS max_items_in_one_order,
    min(add_to_cart_order) AS min_items_in_one_order
FROM analytics.raw_orders;

-- D3. Order distribution per day in a week
SELECT
    order_dow,
    CASE order_dow
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    count(*) AS total_orders
FROM analytics.raw_orders
GROUP BY
    order_dow
ORDER BY order_dow;

-- D4. Top 10 most active user by order count
SELECT
    user_id,
    count(DISTINCT order_id) AS total_orders,
    count(*) AS total_items,
    round(
        avg(days_since_prior_order),
        1
    ) AS avg_days_between_orders
FROM analytics.raw_orders
GROUP BY
    user_id
ORDER BY total_orders DESC
LIMIT 10;

-- D5. Product reorder rate distribution
SELECT
    department,
    COUNT(*) AS total_items,
    SUM(reordered) AS reordered_items,
    ROUND(
        SUM(reordered) * 100.0 / COUNT(*),
        2
    ) AS reorder_rate_pct
FROM analytics.raw_orders
GROUP BY
    department
ORDER BY reorder_rate_pct DESC;

-- D6. Top 10 most popular Aisle
SELECT
    aisle,
    department,
    count(DISTINCT order_id) AS unique_orders,
    count(*) AS total_items
FROM analytics.raw_orders
GROUP BY
    aisle,
    department
ORDER BY unique_orders DESC
LIMIT 10;

-- D7. The most consistent user reorder
SELECT
    user_id,
    COUNT(*) AS total_items,
    ROUND(AVG(reordered) * 100, 2) AS reorder_rate_pct,
    COUNT(DISTINCT product_id) AS unique_products
FROM analytics.raw_orders
GROUP BY
    user_id
HAVING
    COUNT(*) >= 50
ORDER BY reorder_rate_pct DESC, total_items DESC
LIMIT 20;

-- D8. Department contribution to total transaction
SELECT
    department,
    COUNT(*) AS total_items,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS contribution_pct
FROM analytics.raw_orders
GROUP BY
    department
ORDER BY contribution_pct DESC;

-- D9. Customer shopping intensity by time
SELECT
    CASE
        WHEN order_hour_of_day BETWEEN 6 AND 10  THEN 'Morning'
        WHEN order_hour_of_day BETWEEN 11 AND 15  THEN 'Afternoon'
        WHEN order_hour_of_day BETWEEN 16 AND 20  THEN 'Evening'
        ELSE 'Night'
    END AS shopping_period,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT user_id) AS active_users,
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT order_id),
        2
    ) AS avg_items_per_order
FROM analytics.raw_orders
GROUP BY
    shopping_period
ORDER BY total_orders DESC;

-- D10. Department with the most loyal customer
WITH
    department_users AS (
        SELECT
            department,
            user_id,
            COUNT(DISTINCT order_id) AS total_orders,
            AVG(reordered) AS reorder_rate
        FROM analytics.raw_orders
        GROUP BY
            department,
            user_id
    )
SELECT
    department,
    ROUND(AVG(total_orders), 2) AS avg_orders_per_user,
    ROUND(AVG(reorder_rate) * 100, 2) AS avg_reorder_rate_pct,
    COUNT(DISTINCT user_id) AS total_users
FROM department_users
GROUP BY
    department
ORDER BY avg_orders_per_user DESC;

-- D11. Customer retention by interval order
SELECT
    CASE
        WHEN days_since_prior_order <= 7 THEN 'Weekly'
        WHEN days_since_prior_order <= 14 THEN 'Biweekly'
        WHEN days_since_prior_order <= 30 THEN 'Monthly'
        ELSE 'Inactive'
    END AS retention_group,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(AVG(reordered) * 100, 2) AS reorder_rate_pct
FROM analytics.raw_orders
WHERE
    days_since_prior_order IS NOT NULL
GROUP BY
    retention_group
ORDER BY total_users DESC;

-- D12. Market basket depth analysis
SELECT
    CASE
        WHEN add_to_cart_order <= 5 THEN 'Small Basket'
        WHEN add_to_cart_order <= 15 THEN 'Medium Basket'
        ELSE 'Large Basket'
    END AS basket_depth,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_items,
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT order_id),
        2
    ) AS avg_items_per_order
FROM analytics.raw_orders
GROUP BY
    basket_depth
ORDER BY avg_items_per_order DESC;