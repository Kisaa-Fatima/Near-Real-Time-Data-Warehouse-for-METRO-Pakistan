use proj;

-- -------------------------------------------- Query 1--
SELECT 
    MONTH(order_date) AS month,
    CASE 
        WHEN DAYOFWEEK(order_date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    product_name,
    SUM(sale) AS total_revenue
FROM dw_transaction
WHERE YEAR(order_date) = 2019
GROUP BY month, day_type, product_name
ORDER BY total_revenue DESC
LIMIT 5;

-- -------------------------------------------- Query 2-------------------------
-- no record with 2017 in original dataset
WITH quarterly_revenue AS (
    SELECT 
        store_id,
        store_name,
        QUARTER(order_date) AS quarter,
        SUM(sale) AS total_revenue
    FROM dw_transaction
    WHERE YEAR(order_date) = 2017
    GROUP BY store_id, store_name, quarter
)
SELECT 
    store_id,
    store_name,
    CONCAT('Q', quarter) AS quarter,
    total_revenue,
    (total_revenue - LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY quarter)) / 
    LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY quarter) * 100 AS growth_rate
FROM quarterly_revenue;

-- -----------------------------------Query 3 -----------------------
SELECT 
    store_id,
    store_name,
    supplier_id,
    supplier_name,
    product_name,
    SUM(sale) AS total_sales
FROM dw_transaction
GROUP BY store_id, store_name, supplier_id, supplier_name, product_name
ORDER BY store_id, supplier_id, product_name;

-- --------------------- Query 4 ----------------------
SELECT 
    product_name,
    CASE 
        WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(order_date) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Winter'
    END AS season,
    SUM(sale) AS total_sales
FROM dw_transaction
GROUP BY product_name, season
ORDER BY product_name, season;

-- ---------------------- Query 5 -------------------
SELECT 
    store_id,
    store_name,
    supplier_id,
    supplier_name,
    MONTH(order_date) AS month,
    SUM(sale) AS monthly_revenue,
    (SUM(sale) - LAG(SUM(sale)) OVER (PARTITION BY store_id, supplier_id ORDER BY MONTH(order_date))) / 
    LAG(SUM(sale)) OVER (PARTITION BY store_id, supplier_id ORDER BY MONTH(order_date)) * 100 AS revenue_volatility
FROM dw_transaction
GROUP BY store_id, store_name, supplier_id, supplier_name, month;

-- ------------------ Query 6 -----------------
-- no combination found in the original dataset
WITH order_product_pairs AS (
    SELECT 
        a.order_id AS order_id,
        a.product_name AS product_a,
        b.product_name AS product_b
    FROM dw_transaction a
    JOIN dw_transaction b 
        ON a.order_id = b.order_id AND a.product_name < b.product_name
)
SELECT 
    product_a,
    product_b,
    COUNT(*) AS frequency
FROM order_product_pairs
GROUP BY product_a, product_b
ORDER BY frequency DESC
LIMIT 5;

-- RECHECKING TO MAKE SURE THE DATASET HAS NO COMBO
SELECT COUNT(DISTINCT order_id) AS orders_with_multiple_products
FROM dw_transaction
GROUP BY order_id
HAVING COUNT(DISTINCT product_id) > 1;

-- Approach 2: considering multiple transactions of a customer as one with different products
WITH customer_product_pairs AS (
    SELECT 
        a.customer_id,
        a.product_id AS product_1,
        b.product_id AS product_2,
        COUNT(*) AS pair_count
    FROM dw_transaction a
    JOIN dw_transaction b
        ON a.customer_id = b.customer_id AND a.product_id < b.product_id
    GROUP BY a.customer_id, a.product_id, b.product_id
)
SELECT 
    product_1,
    product_2,
    SUM(pair_count) AS total_pair_count
FROM customer_product_pairs
GROUP BY product_1, product_2
ORDER BY total_pair_count DESC
LIMIT 5;

-- ---------------------- Query 7 --------------
-- NUll is intentional and required
SELECT 
    store_id,
    supplier_id,
    product_name,
    SUM(sale) AS total_revenue
FROM dw_transaction
GROUP BY store_id, supplier_id, product_name WITH ROLLUP
ORDER BY store_id, supplier_id, product_name;

-- -------------- Query 8 --------------
SELECT 
    product_name,
    CASE 
        WHEN MONTH(order_date) <= 6 THEN 'H1'
        ELSE 'H2'
    END AS half_year,
    SUM(sale) AS total_revenue,
    SUM(quantity_ordered) AS total_quantity
FROM dw_transaction
GROUP BY product_name, half_year
ORDER BY product_name, half_year;

-- ------------- Query 9 ------------------
WITH product_sales AS (
    SELECT 
        product_id,
        AVG(sale) AS avg_sale,
        COUNT(*) AS total_sales
    FROM dw_transaction
    GROUP BY product_id
),
daily_sales AS (
    SELECT 
        product_id,
        order_date,
        SUM(sale) AS daily_total
    FROM dw_transaction
    GROUP BY product_id, order_date
)
SELECT 
    ds.product_id,
    ds.order_date,
    ds.daily_total,
    ps.avg_sale
FROM daily_sales ds
JOIN product_sales ps
ON ds.product_id = ps.product_id
WHERE ds.daily_total > 2 * ps.avg_sale
ORDER BY ds.product_id, ds.order_date;

-- ------------ Query 10 ---------------
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    store_id,
    store_name,
    CONCAT('Q', QUARTER(order_date)) AS quarter,
    SUM(sale) AS quarterly_sales
FROM dw_transaction
GROUP BY store_id, store_name, quarter
ORDER BY store_name, quarter;

SELECT * FROM STORE_QUARTERLY_SALES;

-- --------------------------------------------------------------------------------------------------------------------------





