-- ============================================================
-- ANALYTICS QUERIES — ClickHouse Warehouse
-- Database: analytics
-- Tables: raw_orders, product_analytics, hourly_analytics,
--         department_analytics
-- ============================================================

-- ============================================================
-- A. PRODUCT ANALYTICS
-- ============================================================

-- A1. Top 10 produk paling banyak dipesan 
-- VISUALISASI: Row chart
SELECT
    product_name,
    department,
    total_orders,
    reorder_count,
    reorder_rate
FROM analytics.product_analytics
ORDER BY total_orders DESC
LIMIT 10;

-- A2. Top 10 produk dengan reorder rate tertinggi (min 5 orders)
-- VISUALISASI: Row chart
SELECT
    product_name,
    department,
    total_orders,
	reorder_count,
    reorder_rate
FROM analytics.product_analytics
WHERE total_orders >= 5
ORDER BY reorder_rate DESC
LIMIT 10;

-- A3. Products with Lowest Reorder Rate
SELECT
    product_name,
    department,
    total_orders,
	reorder_count,
    reorder_rate
FROM analytics.product_analytics
ORDER BY reorder_rate ASC
LIMIT 10;

-- A4. Jumlah produk unik per department
SELECT
    department,
    count(product_name) AS total_products,
    sum(total_orders)   AS total_orders_dept
FROM analytics.product_analytics
GROUP BY department
ORDER BY total_orders_dept DESC;

-- A5. Rata-rata reorder rate per department
SELECT
    department,
    round(avg(reorder_rate), 4) AS avg_reorder_rate,
    sum(total_orders)           AS total_orders
FROM analytics.product_analytics
GROUP BY department
ORDER BY avg_reorder_rate DESC;

-- A6. Top 5 produk per department (window function)
SELECT
    department,
    product_name,
    total_orders,
    reorder_rate,
    rank() OVER (PARTITION BY department ORDER BY total_orders DESC) AS rank_in_dept
FROM analytics.product_analytics
WHERE rank_in_dept <= 5
ORDER BY department, rank_in_dept;

-- A7. Produk "evergreen" — order tinggi DAN reorder rate tinggi
SELECT
    product_name,
    department,
    total_orders,
    reorder_rate,
    round(total_orders * reorder_rate, 2) AS loyalty_score
FROM analytics.product_analytics
WHERE total_orders >= 30
ORDER BY loyalty_score DESC
LIMIT 15;

-- A8. Distribusi reorder rate (bucketing)
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
GROUP BY reorder_bucket
ORDER BY total_orders DESC;

-- A9. Perbandingan total order vs reorder per department (share analysis)
SELECT
    department,
    sum(total_orders)   AS total_orders,
    sum(reorder_count)  AS total_reorders,
    round(sum(reorder_count) / sum(total_orders), 4) AS dept_reorder_rate
FROM analytics.product_analytics
GROUP BY department
ORDER BY dept_reorder_rate DESC;

-- A10. Produk baru (first-time buyer dominant) — reorder rate < 30%
SELECT
    product_name,
    department,
    total_orders,
    reorder_rate,
    (total_orders - reorder_count) AS new_buyer_orders
FROM analytics.product_analytics
WHERE reorder_rate < 0.3
  AND total_orders >= 10
ORDER BY new_buyer_orders DESC
LIMIT 15;


-- ============================================================
-- B. HOURLY ANALYTICS
-- ============================================================

-- B1. Distribusi order per jam (full day)
SELECT
    order_hour_of_day,
    total_orders,
    round(total_orders * 100.0 / sum(total_orders) OVER (), 2) AS pct_of_day
FROM analytics.hourly_analytics
ORDER BY order_hour_of_day;

-- B2. Peak hour — jam tersibuk
SELECT
    order_hour_of_day,
    total_orders
FROM analytics.hourly_analytics
ORDER BY total_orders DESC
LIMIT 5;

-- B3. Off-peak hours — jam paling sepi
SELECT
    order_hour_of_day,
    total_orders
FROM analytics.hourly_analytics
ORDER BY total_orders ASC
LIMIT 5;

-- B4. Segmentasi waktu (Morning / Afternoon / Evening / Night)
SELECT
    CASE
        WHEN order_hour_of_day BETWEEN 5  AND 11 THEN 'Morning (05-11)'
        WHEN order_hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
        WHEN order_hour_of_day BETWEEN 18 AND 21 THEN 'Evening (18-21)'
        ELSE 'Night (22-04)'
    END AS time_segment,
    sum(total_orders)  AS total_orders,
    round(sum(total_orders) * 100.0 / sum(sum(total_orders)) OVER (), 2) AS pct
FROM analytics.hourly_analytics
GROUP BY time_segment
ORDER BY total_orders DESC;

-- B5. Running total order sepanjang hari
SELECT
    order_hour_of_day,
    total_orders,
    sum(total_orders) OVER (ORDER BY order_hour_of_day) AS cumulative_orders
FROM analytics.hourly_analytics
ORDER BY order_hour_of_day;

-- B6. Perbandingan jam vs rata-rata harian (above/below average)
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

-- B7. Top 3 jam per segmen waktu
SELECT
    time_segment,
    order_hour_of_day,
    total_orders,
    rank_in_segment
FROM (
    SELECT
        order_hour_of_day,
        total_orders,
        CASE
            WHEN order_hour_of_day BETWEEN 5  AND 11 THEN 'Morning'
            WHEN order_hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN order_hour_of_day BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_segment,
        rank() OVER (
            PARTITION BY CASE
                WHEN order_hour_of_day BETWEEN 5  AND 11 THEN 'Morning'
                WHEN order_hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
                WHEN order_hour_of_day BETWEEN 18 AND 21 THEN 'Evening'
                ELSE 'Night'
            END
            ORDER BY total_orders DESC
        ) AS rank_in_segment
    FROM analytics.hourly_analytics
)
WHERE rank_in_segment <= 3
ORDER BY time_segment, rank_in_segment;


-- ============================================================
-- C. DEPARTMENT ANALYTICS
-- ============================================================

-- C1. Ranking department berdasarkan total order
SELECT
    department,
    total_orders,
    round(total_orders * 100.0 / sum(total_orders) OVER (), 2) AS market_share_pct
FROM analytics.department_analytics
ORDER BY total_orders DESC;

-- C2. Top 5 department
SELECT
    department,
    total_orders
FROM analytics.department_analytics
ORDER BY total_orders DESC
LIMIT 5;

-- C3. Department terbawah (least popular)
SELECT
    department,
    total_orders
FROM analytics.department_analytics
ORDER BY total_orders ASC
LIMIT 5;

-- C4. Kumulatif market share (sampai 80% = Pareto)
SELECT
    department,
    total_orders,
    round(sum(total_orders) OVER (ORDER BY total_orders DESC) * 100.0
          / sum(total_orders) OVER (), 2) AS cumulative_pct
FROM analytics.department_analytics
ORDER BY total_orders DESC;

-- C5. Department di atas dan di bawah rata-rata
SELECT
    department,
    total_orders,
    round(avg(total_orders) OVER (), 2) AS avg_orders,
    CASE
        WHEN total_orders > avg(total_orders) OVER () THEN 'Above Average'
        ELSE 'Below Average'
    END AS performance
FROM analytics.department_analytics
ORDER BY total_orders DESC;


-- ============================================================
-- D. RAW ORDERS — Transactional Deep Dive
-- ============================================================

-- D1. Total keseluruhan transaksi
SELECT
    count(*)                    AS total_rows,
    count(DISTINCT order_id)    AS unique_orders,
    count(DISTINCT user_id)     AS unique_users,
    count(DISTINCT product_id)  AS unique_products
FROM analytics.raw_orders;

-- D2. Distribusi order per hari dalam seminggu
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
GROUP BY order_dow
ORDER BY order_dow;

-- D3. User paling aktif (top 10 by order count)
SELECT
    user_id,
    count(DISTINCT order_id)    AS total_orders,
    count(*)                    AS total_items,
    round(avg(days_since_prior_order), 1) AS avg_days_between_orders
FROM analytics.raw_orders
GROUP BY user_id
ORDER BY total_orders DESC
LIMIT 10;

-- D4. Rata-rata item per order
SELECT
    round(count(*) / count(DISTINCT order_id), 2) AS avg_items_per_order,
    max(add_to_cart_order)                         AS max_items_in_one_order,
    min(add_to_cart_order)                         AS min_items_in_one_order
FROM analytics.raw_orders;

-- D5. Reorder rate keseluruhan dari raw data
SELECT
    count(*)                                              AS total_items,
    sum(reordered)                                        AS reordered_items,
    round(sum(reordered) * 100.0 / count(*), 2)           AS reorder_rate_pct
FROM analytics.raw_orders;

-- D6. Pola order per jam dari raw data (cross-check dengan hourly_analytics)
SELECT
    order_hour_of_day,
    count(DISTINCT order_id) AS unique_orders,
    count(*)                 AS total_items
FROM analytics.raw_orders
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;

-- D7. Department popularity dari raw data (cross-check dengan department_analytics)
SELECT
    department,
    count(DISTINCT order_id) AS unique_orders,
    count(*)                 AS total_items,
    round(avg(reordered), 4) AS reorder_rate
FROM analytics.raw_orders
GROUP BY department
ORDER BY unique_orders DESC;

-- D8. Distribusi days_since_prior_order (seberapa sering user kembali)
SELECT
    days_since_prior_order,
    count(*) AS frequency
FROM analytics.raw_orders
WHERE days_since_prior_order IS NOT NULL
GROUP BY days_since_prior_order
ORDER BY days_since_prior_order;

-- D9. Aisle terpopuler
SELECT
    aisle,
    department,
    count(DISTINCT order_id) AS unique_orders,
    count(*)                 AS total_items
FROM analytics.raw_orders
GROUP BY aisle, department
ORDER BY unique_orders DESC
LIMIT 15;

-- D10. Kombinasi DOW + hour dengan order terbanyak (heatmap data)
SELECT
    order_dow,
    order_hour_of_day,
    count(DISTINCT order_id) AS unique_orders
FROM analytics.raw_orders
GROUP BY order_dow, order_hour_of_day
ORDER BY unique_orders DESC
LIMIT 20;