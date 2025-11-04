/*
===============================================================================
Quality Checks
===============================================================================
Usage: run after loading the Silver layer
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- Nulls or Duplicates in (assumed) PK cst_id  — Expect: no rows
SELECT 
  cst_id,
  COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Unwanted spaces — Expect: no rows
SELECT 
  cst_key 
FROM silver.crm_cust_info
WHERE cst_key <> TRIM(cst_key);

-- Data standardization & consistency (enumerate values)
SELECT DISTINCT 
  cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

-- Nulls or Duplicates in (assumed) PK prd_id  — Expect: no rows
SELECT 
  prd_id,
  COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Unwanted spaces — Expect: no rows
SELECT 
  prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- Null or negative cost — Expect: no rows
SELECT 
  prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data standardization & consistency (enumerate values)
SELECT DISTINCT 
  prd_line 
FROM silver.crm_prd_info;

-- Invalid date orders (start > end) — Expect: no rows
SELECT 
  * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

-- Invalid raw dates in bronze (yyyymmdd as int) — Expect: no rows
SELECT 
  NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
   OR length(sls_due_dt::text) <> 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;

-- Invalid date orders in silver (order > ship/due) — Expect: no rows
SELECT 
  * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Consistency: sales = quantity * price and values > 0 — Expect: no rows
SELECT DISTINCT 
  sls_sales,
  sls_quantity,
  sls_price 
FROM silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- Out-of-range birthdates — Expect: between 1924-01-01 and today
SELECT DISTINCT 
  bdate 
FROM silver.erp_cust_az12
WHERE bdate < DATE '1924-01-01' 
   OR bdate > CURRENT_DATE;

-- Data standardization & consistency (enumerate values)
SELECT DISTINCT 
  gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- Data standardization & consistency (enumerate values)
SELECT DISTINCT 
  cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Unwanted spaces — Expect: no rows
SELECT 
  * 
FROM silver.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) 
   OR subcat <> TRIM(subcat) 
   OR maintenance <> TRIM(maintenance);

-- Data standardization & consistency (enumerate values)
SELECT DISTINCT 
  maintenance 
FROM silver.erp_px_cat_g1v2;