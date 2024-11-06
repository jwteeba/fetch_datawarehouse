/**
Business Requirements:
What are the top 5 brands by receipts scanned for most recent month?
How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
Which brand has the most spend among users who were created within the past 6 months?
Which brand has the most transactions among users who were created within the past 6 months?

SQL Syntax: AWS Redshift
**/

-- What are the top 5 brands by receipts scanned for most recent month?
WITH recent_month AS (
    SELECT MAX(date_id) AS max_date_id
    FROM DIM_DATE
    WHERE date_id = EXTRACT(YEAR FROM GETDATE()) * 10000 + EXTRACT(MONTH FROM GETDATE()) * 100 + 1
    --
)
SELECT 
    b.name AS brand_name,
    COUNT(DISTINCT f.receipt_id) AS receipt_count
FROM FACT_RECEIPT f
JOIN FACT_RECEIPT_ITEM fi ON f.receipt_id = fi.receipt_id
JOIN DIM_BRAND b ON fi.brand_id = b.brand_id
JOIN recent_month rm ON f.scan_date_id >= rm.max_date_id 
    AND f.scan_date_id < rm.max_date_id + 100
GROUP BY b.name
ORDER BY receipt_count DESC
LIMIT 5;

-- How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?



WITH recent_months AS (
    SELECT 
        MAX(date_id) AS current_month,
        MAX(date_id) - 100 AS previous_month
    FROM DIM_DATE
    WHERE date_id = EXTRACT(YEAR FROM GETDATE()) * 10000 + EXTRACT(MONTH FROM GETDATE()) * 100 + 1
    -- EXTRACT(YEAR FROM GETDATE()) * 10000: This shifts the year to the ten-thousands place. For 2024, this becomes 20240000.
    -- EXTRACT(MONTH FROM GETDATE()) * 100: This shifts the month to the hundreds place. For November (11), this becomes 1100.
    -- Adding 1 at the end represents the first day of the month.
    -- By using this calculation in the WHERE clause, I am selecting the row in DIM_DATE that corresponds to the first day of the current month. This is useful for finding the start of the current month, which can then be used as a reference point for further date calculations or filtering.
    -- This avoids the need for more complex date manipulations and can utilize integer comparisons, which are generally faster than date comparisons.
),
brand_ranks AS (
    SELECT 
        b.name AS brand_name,
        CASE 
            WHEN f.scan_date_id >= rm.current_month AND f.scan_date_id < rm.current_month + 100 
            THEN 'Current' 
            ELSE 'Previous' 
        END AS month_type,
        COUNT(DISTINCT f.receipt_id) AS receipt_count,
        ROW_NUMBER() OVER (
            PARTITION BY 
                CASE 
                    WHEN f.scan_date_id >= rm.current_month AND f.scan_date_id < rm.current_month + 100 
                    THEN 'Current' 
                    ELSE 'Previous' 
                END
            ORDER BY COUNT(DISTINCT f.receipt_id) DESC
        ) AS rank
    FROM FACT_RECEIPT f
    JOIN FACT_RECEIPT_ITEM fi ON f.receipt_id = fi.receipt_id
    JOIN DIM_BRAND b ON fi.brand_id = b.brand_id
    CROSS JOIN recent_months rm
    WHERE f.scan_date_id >= rm.previous_month AND f.scan_date_id < rm.current_month + 100
    GROUP BY b.name, month_type
)
SELECT 
    c.brand_name,
    c.receipt_count AS current_receipt_count,
    c.rank AS current_rank,
    p.receipt_count AS previous_receipt_count,
    p.rank AS previous_rank,
    p.rank - c.rank AS rank_change
FROM brand_ranks c
JOIN brand_ranks p ON c.brand_name = p.brand_name AND c.month_type = 'Current' AND p.month_type = 'Previous'
WHERE c.rank <= 5 OR p.rank <= 5
ORDER BY c.rank;


-- When considering average spend from receipts with 'rewardsReceiptStatus' of 'Accepted' or 'Rejected', which is greater?

SELECT 
    receipt_status,
    AVG(total_spent) AS avg_spend
FROM FACT_RECEIPT
WHERE receipt_status IN ('ACCEPTED', 'REJECTED')
GROUP BY receipt_status
ORDER BY avg_spend DESC
LIMIT 1;

-- When considering total number of items purchased from receipts with 'rewardsReceiptStatus' of 'Accepted' or 'Rejected', which is greater?

SELECT 
    receipt_status,
    SUM(purchased_item_count) AS total_items_purchased
FROM FACT_RECEIPT
WHERE receipt_status IN ('ACCEPTED', 'REJECTED')
GROUP BY receipt_status
ORDER BY total_items_purchased DESC
LIMIT 1;


-- Which brand has the most spend among users who were created within the past 6 months?

WITH recent_users AS (
    SELECT user_id
    FROM DIM_USER
    WHERE created_date >= DATEADD(month, -6, GETDATE())
)
SELECT 
    b.name AS brand_name,
    SUM(fi.final_price * fi.quantity_purchased) AS total_spend
FROM FACT_RECEIPT f
JOIN FACT_RECEIPT_ITEM fi ON f.receipt_id = fi.receipt_id
JOIN DIM_BRAND b ON fi.brand_id = b.brand_id
JOIN recent_users ru ON f.user_id = ru.user_id
GROUP BY b.name
ORDER BY total_spend DESC
LIMIT 1;

-- Which brand has the most transactions among users who were created within the past 6 months?

WITH recent_users AS (
    SELECT user_id
    FROM DIM_USER
    WHERE created_date >= DATEADD(month, -6, GETDATE())
)
SELECT 
    b.name AS brand_name,
    COUNT(DISTINCT f.receipt_id) AS transaction_count
FROM FACT_RECEIPT f
JOIN FACT_RECEIPT_ITEM fi ON f.receipt_id = fi.receipt_id
JOIN DIM_BRAND b ON fi.brand_id = b.brand_id
JOIN recent_users ru ON f.user_id = ru.user_id
GROUP BY b.name
ORDER BY transaction_count DESC
LIMIT 1;
