-- SQL Syntax: AWS Redshift

-- Check for null values in important fields:

SELECT 
    COUNT(*) - COUNT(user_id) AS null_user_id,
    COUNT(*) - COUNT(active) AS null_active,
    COUNT(*) - COUNT(created_date) AS null_created_date,
    COUNT(*) - COUNT(role) AS null_role,
    COUNT(*) - COUNT(state) AS null_state
FROM DIM_USER;

SELECT 
    COUNT(*) - COUNT(receipt_id) AS null_receipt_id,
    COUNT(*) - COUNT(user_id) AS null_user_id,
    COUNT(*) - COUNT(purchase_date_id) AS null_purchase_date_id,
    COUNT(*) - COUNT(total_spent) AS null_total_spent,
    COUNT(*) - COUNT(receipt_status) AS null_receipt_status
FROM FACT_RECEIPT;

-- Check for data consistency in FACT_RECEIPT:

SELECT 
    receipt_id,
    total_spent,
    purchased_item_count,
    (SELECT SUM(final_price * quantity_purchased) 
     FROM FACT_RECEIPT_ITEM 
     WHERE FACT_RECEIPT_ITEM.receipt_id = FACT_RECEIPT.receipt_id) AS calculated_total
FROM FACT_RECEIPT
WHERE ABS(total_spent - (SELECT SUM(final_price * quantity_purchased) 
                         FROM FACT_RECEIPT_ITEM 
                         WHERE FACT_RECEIPT_ITEM.receipt_id = FACT_RECEIPT.receipt_id)) > 0.01;


-- Check for invalid dates:

SELECT COUNT(*) AS invalid_dates
FROM DIM_DATE
WHERE year < 2000 OR year > EXTRACT(YEAR FROM CURRENT_DATE) + 1
   OR month < 1 OR month > 12
   OR day < 1 OR day > 31;

-- Check for duplicate brand codes:

SELECT brand_code, COUNT(*) AS count
FROM DIM_BRAND
GROUP BY brand_code
HAVING COUNT(*) > 1;

-- Check for users with unreasonably high number of receipts
SELECT u.user_id, u.state, COUNT(r.receipt_id) AS receipt_count
FROM DIM_USER u
JOIN FACT_RECEIPT r ON u.user_id = r.user_id
GROUP BY u.user_id, u.state
HAVING COUNT(r.receipt_id) > 1000
ORDER BY receipt_count DESC;

-- Check for receipts with unusually high total spent

SELECT receipt_id, user_id, total_spent
FROM FACT_RECEIPT
WHERE total_spent > 1000
ORDER BY total_spent DESC
LIMIT 10;

-- Check for consistency between FACT_RECEIPT and FACT_RECEIPT_ITEM:

SELECT fr.receipt_id, fr.purchased_item_count, COUNT(fi.barcode) AS actual_item_count
FROM FACT_RECEIPT fr
LEFT JOIN FACT_RECEIPT_ITEM fi ON fr.receipt_id = fi.receipt_id
GROUP BY fr.receipt_id, fr.purchased_item_count
HAVING fr.purchased_item_count <> COUNT(fi.barcode);

-- Check for brands without a valid CPG:

SELECT COUNT(*) AS brands_without_cpg
FROM DIM_BRAND b
LEFT JOIN DIM_CPG c ON b.cpg_id = c.cpg_id
WHERE c.cpg_id IS NULL;

-- Check for unusual time gaps between purchase and scan dates:

SELECT r.receipt_id, 
       d1.full_date AS purchase_date, 
       d2.full_date AS scan_date,
       d2.full_date - d1.full_date AS days_difference
FROM FACT_RECEIPT r
JOIN DIM_DATE d1 ON r.purchase_date_id = d1.date_id
JOIN DIM_DATE d2 ON r.scan_date_id = d2.date_id
WHERE d2.full_date - d1.full_date > 30
ORDER BY days_difference DESC;


-- Check for orphaned records in FACT_RECEIPT_ITEM:

SELECT COUNT(*) AS orphaned_items
FROM FACT_RECEIPT_ITEM fi
LEFT JOIN FACT_RECEIPT fr ON fi.receipt_id = fr.receipt_id
WHERE fr.receipt_id IS NULL;




