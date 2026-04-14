-- =============================================================================
-- PROJECT 4: Data Quality & Validation Pipeline
-- SQL Quality Check Queries
-- Database: sales_data (SQLite / PostgreSQL compatible)
-- =============================================================================
-- Run these queries against the raw ingested table to catch issues
-- before they enter the data warehouse / reporting layer.
-- =============================================================================

-- ─── TABLE CREATION ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sales_raw (
    order_id       TEXT,
    customer_id    TEXT,
    customer_name  TEXT,
    email          TEXT,
    phone          TEXT,
    province       TEXT,
    category       TEXT,
    product_name   TEXT,
    quantity       TEXT,   -- intentionally TEXT to catch type issues
    unit_price     TEXT,   -- intentionally TEXT to catch type issues
    discount_pct   TEXT,
    order_date     TEXT,
    ship_date      TEXT,
    status         TEXT,
    sales_rep      TEXT,
    revenue        TEXT
);

-- =============================================================================
-- SECTION 1: COMPLETENESS CHECKS
-- =============================================================================

-- 1.1 Missing values count per column
SELECT
    'order_id'    AS column_name, COUNT(*) - COUNT(order_id)    AS null_count FROM sales_raw
UNION ALL SELECT 'customer_id',  COUNT(*) - COUNT(customer_id)  FROM sales_raw
UNION ALL SELECT 'email',        COUNT(*) - COUNT(email)        FROM sales_raw
UNION ALL SELECT 'province',     COUNT(*) - COUNT(province)     FROM sales_raw
UNION ALL SELECT 'unit_price',   COUNT(*) - COUNT(unit_price)   FROM sales_raw
UNION ALL SELECT 'quantity',     COUNT(*) - COUNT(quantity)     FROM sales_raw
UNION ALL SELECT 'order_date',   COUNT(*) - COUNT(order_date)   FROM sales_raw
ORDER BY null_count DESC;

-- 1.2 Completeness rate per column (%)
SELECT
    column_name,
    null_count,
    total_rows,
    ROUND(100.0 * (total_rows - null_count) / total_rows, 2) AS completeness_pct
FROM (
    SELECT 'email' AS column_name,
           SUM(CASE WHEN email IS NULL OR TRIM(email) = '' THEN 1 ELSE 0 END) AS null_count,
           COUNT(*) AS total_rows
    FROM sales_raw
    UNION ALL
    SELECT 'phone',
           SUM(CASE WHEN phone IS NULL OR TRIM(phone) = '' THEN 1 ELSE 0 END),
           COUNT(*) FROM sales_raw
    UNION ALL
    SELECT 'province',
           SUM(CASE WHEN province IS NULL OR TRIM(province) = '' THEN 1 ELSE 0 END),
           COUNT(*) FROM sales_raw
) t
ORDER BY completeness_pct ASC;

-- =============================================================================
-- SECTION 2: UNIQUENESS CHECKS
-- =============================================================================

-- 2.1 Duplicate order IDs
SELECT
    order_id,
    COUNT(*) AS duplicate_count
FROM sales_raw
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

-- 2.2 Full duplicate rows (all key fields match)
SELECT
    order_id, customer_id, order_date, unit_price, quantity,
    COUNT(*) AS occurrences
FROM sales_raw
GROUP BY order_id, customer_id, order_date, unit_price, quantity
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- 2.3 Duplicate emails (same customer multiple IDs)
SELECT
    email,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    COUNT(*) AS total_records
FROM sales_raw
WHERE email IS NOT NULL AND TRIM(email) != ''
GROUP BY email
HAVING COUNT(DISTINCT customer_id) > 1
ORDER BY distinct_customers DESC
LIMIT 20;

-- =============================================================================
-- SECTION 3: VALIDITY CHECKS
-- =============================================================================

-- 3.1 Invalid email format
SELECT
    order_id, customer_id, email,
    'INVALID_EMAIL' AS issue_type
FROM sales_raw
WHERE email IS NOT NULL
  AND email NOT LIKE '%_@__%.__%'
  AND TRIM(email) != ''
ORDER BY customer_id;

-- 3.2 Negative or zero unit prices
SELECT
    order_id, customer_id, unit_price,
    'INVALID_PRICE' AS issue_type
FROM sales_raw
WHERE CAST(unit_price AS REAL) <= 0
   OR unit_price IS NULL;

-- 3.3 Extreme price outliers (> 3× the 95th percentile)
WITH price_stats AS (
    SELECT AVG(CAST(unit_price AS REAL)) AS avg_price,
           MAX(CAST(unit_price AS REAL)) AS max_price
    FROM sales_raw
    WHERE CAST(unit_price AS REAL) > 0
)
SELECT
    s.order_id, s.customer_id, s.unit_price,
    p.avg_price,
    CAST(s.unit_price AS REAL) / p.avg_price AS price_multiple,
    'PRICE_OUTLIER' AS issue_type
FROM sales_raw s, price_stats p
WHERE CAST(s.unit_price AS REAL) > p.avg_price * 50
ORDER BY CAST(s.unit_price AS REAL) DESC;

-- 3.4 Invalid quantity (non-numeric or zero/negative)
SELECT
    order_id, customer_id, quantity,
    'INVALID_QUANTITY' AS issue_type
FROM sales_raw
WHERE CAST(quantity AS REAL) IS NULL
   OR CAST(quantity AS REAL) <= 0
   OR quantity LIKE '%[^0-9]%';

-- 3.5 Invalid discount percentage (< 0 or > 100)
SELECT
    order_id, customer_id, discount_pct,
    'INVALID_DISCOUNT' AS issue_type
FROM sales_raw
WHERE CAST(discount_pct AS REAL) < 0
   OR CAST(discount_pct AS REAL) > 100;

-- =============================================================================
-- SECTION 4: CONSISTENCY CHECKS
-- =============================================================================

-- 4.1 Ship date before order date (logical violation)
SELECT
    order_id, customer_id,
    order_date, ship_date,
    JULIANDAY(ship_date) - JULIANDAY(order_date) AS days_diff,
    'SHIP_BEFORE_ORDER' AS issue_type
FROM sales_raw
WHERE order_date IS NOT NULL
  AND ship_date IS NOT NULL
  AND ship_date < order_date
ORDER BY days_diff ASC;

-- 4.2 Future order dates (beyond data collection end)
SELECT
    order_id, customer_id, order_date,
    'FUTURE_DATE' AS issue_type
FROM sales_raw
WHERE DATE(order_date) > DATE('2024-07-01')
ORDER BY order_date DESC;

-- 4.3 Province non-standard values
SELECT
    province,
    COUNT(*) AS occurrences,
    'NON_STANDARD_PROVINCE' AS issue_type
FROM sales_raw
WHERE province NOT IN (
    'Ontario','Quebec','British Columbia','Alberta',
    'Manitoba','Saskatchewan','Nova Scotia','New Brunswick',
    'Prince Edward Island','Newfoundland and Labrador',
    'Northwest Territories','Nunavut','Yukon'
)
GROUP BY province
ORDER BY occurrences DESC;

-- 4.4 Revenue cross-validation (calculated vs stored)
SELECT
    order_id, customer_id,
    ROUND(CAST(quantity AS REAL) * CAST(unit_price AS REAL) *
          (1 - CAST(discount_pct AS REAL)/100), 2) AS calculated_revenue,
    CAST(revenue AS REAL) AS stored_revenue,
    ABS(ROUND(CAST(quantity AS REAL) * CAST(unit_price AS REAL) *
              (1 - CAST(discount_pct AS REAL)/100), 2) - CAST(revenue AS REAL)) AS discrepancy,
    'REVENUE_MISMATCH' AS issue_type
FROM sales_raw
WHERE ABS(
    ROUND(CAST(quantity AS REAL) * CAST(unit_price AS REAL) *
          (1 - CAST(discount_pct AS REAL)/100), 2)
    - CAST(revenue AS REAL)
) > 0.01
LIMIT 50;

-- =============================================================================
-- SECTION 5: DATA QUALITY SUMMARY DASHBOARD QUERY
-- =============================================================================

-- Master DQ Summary — one row per check, for dashboard
WITH checks AS (
    SELECT 'Total Records'       AS check_name, 'INFO'     AS severity, COUNT(*) AS affected FROM sales_raw
    UNION ALL
    SELECT 'Missing Emails',      'CRITICAL',
           SUM(CASE WHEN email IS NULL OR TRIM(email)='' THEN 1 ELSE 0 END) FROM sales_raw
    UNION ALL
    SELECT 'Invalid Prices',      'CRITICAL',
           SUM(CASE WHEN CAST(unit_price AS REAL) <= 0 THEN 1 ELSE 0 END) FROM sales_raw
    UNION ALL
    SELECT 'Future Dates',        'CRITICAL',
           SUM(CASE WHEN DATE(order_date) > '2024-07-01' THEN 1 ELSE 0 END) FROM sales_raw
    UNION ALL
    SELECT 'Ship Before Order',   'CRITICAL',
           SUM(CASE WHEN ship_date < order_date THEN 1 ELSE 0 END) FROM sales_raw
    UNION ALL
    SELECT 'Invalid Discount',    'WARNING',
           SUM(CASE WHEN CAST(discount_pct AS REAL) NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END)
           FROM sales_raw
)
SELECT
    check_name,
    severity,
    affected,
    ROUND(100.0 * affected / (SELECT COUNT(*) FROM sales_raw), 2) AS pct_of_total
FROM checks
ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END, affected DESC;

-- =============================================================================
-- SECTION 6: REMEDIATION / CLEANING QUERIES
-- =============================================================================

-- 6.1 Create cleaned table
CREATE TABLE IF NOT EXISTS sales_clean AS
SELECT
    order_id,
    customer_id,
    TRIM(customer_name) AS customer_name,
    LOWER(TRIM(email)) AS email,
    phone,
    -- Standardise province
    CASE province
        WHEN 'ON' THEN 'Ontario' WHEN 'ontario' THEN 'Ontario'
        WHEN 'Ont.' THEN 'Ontario' WHEN 'ONTARIO' THEN 'Ontario'
        WHEN 'QC' THEN 'Quebec' WHEN 'quebec' THEN 'Quebec'
        WHEN 'BC' THEN 'British Columbia' WHEN 'AB' THEN 'Alberta'
        WHEN 'MB' THEN 'Manitoba' WHEN 'SK' THEN 'Saskatchewan'
        ELSE province
    END AS province,
    category,
    product_name,
    ABS(CAST(quantity AS INTEGER)) AS quantity,
    ABS(CAST(unit_price AS REAL)) AS unit_price,
    CASE WHEN CAST(discount_pct AS REAL) BETWEEN 0 AND 100
         THEN CAST(discount_pct AS REAL) ELSE 0 END AS discount_pct,
    order_date,
    ship_date,
    status,
    sales_rep
FROM sales_raw
WHERE
    -- Exclude critical violations
    CAST(unit_price AS REAL) > 0
    AND CAST(quantity AS REAL) > 0
    AND DATE(order_date) <= DATE('2024-07-01')
    AND (ship_date IS NULL OR ship_date >= order_date)
    AND customer_id IS NOT NULL
    AND order_id IS NOT NULL
    -- Remove duplicates
    AND rowid IN (
        SELECT MIN(rowid)
        FROM sales_raw
        GROUP BY order_id, customer_id, order_date
    );

-- 6.2 Verify clean table
SELECT
    COUNT(*) AS clean_records,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM sales_raw), 1) AS retention_pct,
    SUM(CAST(quantity AS REAL) * CAST(unit_price AS REAL) *
        (1 - CAST(discount_pct AS REAL)/100)) AS total_revenue
FROM sales_clean;
