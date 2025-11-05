CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE PROCEDURE silver.validate_silver()
LANGUAGE plpgsql
AS $$
/*
Purpose:
  Run SILVER-layer data quality checks and summarize results.

What it checks & where results go (temp tables):
  1) crm_cust_info:
     - duplicates/null PK (cst_id)     -> tmp_silver_dup_cst_id
     - key has unwanted spaces         -> tmp_silver_spaces_cst_key
     - (enum listing only)             -> SELECT DISTINCT cst_marital_status ...

  2) crm_prd_info:
     - duplicates/null PK (prd_id)     -> tmp_silver_dup_prd_id
     - name has unwanted spaces        -> tmp_silver_spaces_prd_nm
     - cost negative / null            -> tmp_silver_bad_prd_cost
     - start/end date order invalid    -> tmp_silver_prd_bad_date_order
     - (enum listing only)             -> SELECT DISTINCT prd_line ...

  3) crm_sales_details:
     - invalid raw dates in bronze     -> tmp_silver_invalid_bronze_dates
     - bad date order in silver        -> tmp_silver_sales_bad_date_order
     - sales ≠ qty*price or nonpositive-> tmp_silver_sales_inconsistency

  4) erp_cust_az12:
     - birthdate out of range          -> tmp_silver_bdate_out_of_range
     - (enum listing only)             -> SELECT DISTINCT gen ...

  5) erp_loc_a101:
     - (enum listing only)             -> SELECT DISTINCT cntry ...

  6) erp_px_cat_g1v2:
     - unwanted spaces in text cols    -> tmp_silver_pxcat_spaces
     - (enum listing only)             -> SELECT DISTINCT maintenance ...

Notes:
  - Temp tables are ON COMMIT DROP (session-scoped)
  - Summary counts printed with RAISE NOTICE
*/
DECLARE
  n_dup_cst_id      bigint := 0;
  n_spaces_cst_key  bigint := 0;

  n_dup_prd_id      bigint := 0;
  n_spaces_prd_nm   bigint := 0;
  n_bad_prd_cost    bigint := 0;
  n_prd_date_order  bigint := 0;

  n_invalid_raw_dt  bigint := 0;
  n_sales_date_ord  bigint := 0;
  n_sales_incons    bigint := 0;

  n_bdate_oor       bigint := 0;
  n_pxcat_spaces    bigint := 0;
BEGIN
  RAISE NOTICE '================ SILVER Validation ================';

  ------------------------------------------------------------------
  -- 1) silver.crm_cust_info
  ------------------------------------------------------------------
  -- duplicates / nulls in cst_id
  DROP TABLE IF EXISTS tmp_silver_dup_cst_id;
  CREATE TEMP TABLE tmp_silver_dup_cst_id ON COMMIT DROP AS
  SELECT cst_id, COUNT(*) AS duplicate_count
  FROM silver.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(*) > 1 OR cst_id IS NULL;

  SELECT COUNT(*) INTO n_dup_cst_id FROM tmp_silver_dup_cst_id;
  IF n_dup_cst_id = 0 THEN
    RAISE NOTICE '[OK] crm_cust_info: cst_id uniqueness ✓';
  ELSE
    RAISE NOTICE '[FAIL] crm_cust_info: % duplicate/null cst_id(s). See tmp_silver_dup_cst_id.', n_dup_cst_id;
  END IF;

  -- unwanted spaces in cst_key
  DROP TABLE IF EXISTS tmp_silver_spaces_cst_key;
  CREATE TEMP TABLE tmp_silver_spaces_cst_key ON COMMIT DROP AS
  SELECT cst_key
  FROM silver.crm_cust_info
  WHERE cst_key <> TRIM(cst_key);

  SELECT COUNT(*) INTO n_spaces_cst_key FROM tmp_silver_spaces_cst_key;
  IF n_spaces_cst_key = 0 THEN
    RAISE NOTICE '[OK] crm_cust_info: cst_key has no unwanted spaces ✓';
  ELSE
    RAISE NOTICE '[FAIL] crm_cust_info: % key(s) with unwanted spaces. See tmp_silver_spaces_cst_key.', n_spaces_cst_key;
  END IF;

  -- (enum) SELECT DISTINCT cst_marital_status ...  (run manually if needed)

  ------------------------------------------------------------------
  -- 2) silver.crm_prd_info
  ------------------------------------------------------------------
  -- duplicates / nulls in prd_id
  DROP TABLE IF EXISTS tmp_silver_dup_prd_id;
  CREATE TEMP TABLE tmp_silver_dup_prd_id ON COMMIT DROP AS
  SELECT prd_id, COUNT(*) AS duplicate_count
  FROM silver.crm_prd_info
  GROUP BY prd_id
  HAVING COUNT(*) > 1 OR prd_id IS NULL;

  SELECT COUNT(*) INTO n_dup_prd_id FROM tmp_silver_dup_prd_id;
  IF n_dup_prd_id = 0 THEN
    RAISE NOTICE '[OK] crm_prd_info: prd_id uniqueness ✓';
  ELSE
    RAISE NOTICE '[FAIL] crm_prd_info: % duplicate/null prd_id(s). See tmp_silver_dup_prd_id.', n_dup_prd_id;
  END IF;

  -- unwanted spaces in prd_nm
  DROP TABLE IF EXISTS tmp_silver_spaces_prd_nm;
  CREATE TEMP TABLE tmp_silver_spaces_prd_nm ON COMMIT DROP AS
  SELECT prd_nm
  FROM silver.crm_prd_info
  WHERE prd_nm <> TRIM(prd_nm);

  SELECT COUNT(*) INTO n_spaces_prd_nm FROM tmp_silver_spaces_prd_nm;
  IF n_spaces_prd_nm = 0 THEN
    RAISE NOTICE '[OK] crm_prd_info: prd_nm has no unwanted spaces ✓';
  ELSE
    RAISE NOTICE '[FAIL] crm_prd_info: % name(s) with unwanted spaces. See tmp_silver_spaces_prd_nm.', n_spaces_prd_nm;
  END IF;

  -- cost negative / null
  DROP TABLE IF EXISTS tmp_silver_bad_prd_cost;
  CREATE TEMP TABLE tmp_silver_bad_prd_cost ON COMMIT DROP AS
  SELECT prd_id, prd_cost
  FROM silver.crm_prd_info
  WHERE prd_cost < 0 OR prd_cost IS NULL;

  SELECT COUNT(*) INTO n_bad_prd_cost FROM tmp_silver_bad_prd_cost;
  IF n_bad_prd_cost = 0 THEN
    RAISE NOTICE '[OK] crm_prd_info: prd_cost valid (nonnegative & not null) ✓';
  ELSE
    RAISE NOTICE '[FAIL] crm_prd_info: % row(s) with invalid cost. See tmp_silver_bad_prd_cost.', n_bad_prd_cost;
  END IF;

  -- invalid date order (end < start)
  DROP TABLE IF EXISTS tmp_silver_prd_bad_date_order;
  CREATE TEMP TABLE tmp_silver_prd_bad_date_order ON COMMIT DROP AS
  SELECT *
  FROM silver.crm_prd_info
  WHERE prd_end_dt < prd_start_dt;

  SELECT COUNT(*) INTO n_prd_date_order FROM tmp_silver_prd_bad_date_order;
  IF n_prd_date_order = 0 THEN
    RAISE NOTICE '[OK] crm_prd_info: start/end date order ✓';
  ELSE
    RAISE NOTICE '[FAIL] crm_prd_info: % row(s) with end<start. See tmp_silver_prd_bad_date_order.', n_prd_date_order;
  END IF;

  -- (enum) SELECT DISTINCT prd_line ... (run manually if needed)

  ------------------------------------------------------------------
  -- 3) silver.crm_sales_details
  ------------------------------------------------------------------
  -- invalid raw dates in bronze (yyyymmdd as int)
  DROP TABLE IF EXISTS tmp_silver_invalid_bronze_dates;
  CREATE TEMP TABLE tmp_silver_invalid_bronze_dates ON COMMIT DROP AS
  SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
  FROM bronze.crm_sales_details
  WHERE sls_due_dt <= 0
     OR length(sls_due_dt::text) <> 8
     OR sls_due_dt > 20500101
     OR sls_due_dt < 19000101;

  SELECT COUNT(*) INTO n_invalid_raw_dt FROM tmp_silver_invalid_bronze_dates;
  IF n_invalid_raw_dt = 0 THEN
    RAISE NOTICE '[OK] crm_sales_details: raw due dates in bronze look valid ✓';
  ELSE
    RAISE NOTICE '[FAIL] crm_sales_details: % invalid raw due date(s) in bronze. See tmp_silver_invalid_bronze_dates.', n_invalid_raw_dt;
  END IF;

  -- invalid date order in silver (order > ship/due)
  DROP TABLE IF EXISTS tmp_silver_sales_bad_date_order;
  CREATE TEMP TABLE tmp_silver_sales_bad_date_order ON COMMIT DROP AS
  SELECT *
  FROM silver.crm_sales_details
  WHERE sls_order_dt > sls_ship_dt
     OR sls_order_dt > sls_due_dt;

  SELECT COUNT(*) INTO n_sales_date_ord FROM tmp_silver_sales_bad_date_order;
  IF n_sales_date_ord = 0 THEN
    RAISE NOTICE '[OK] crm_sales_details: order/ship/due date order ✓';
  ELSE
    RAISE NOTICE '[FAIL] crm_sales_details: % row(s) with bad date order. See tmp_silver_sales_bad_date_order.', n_sales_date_ord;
  END IF;

  -- sales consistency (sales = qty*price, fields positive & not null)
  DROP TABLE IF EXISTS tmp_silver_sales_inconsistency;
  CREATE TEMP TABLE tmp_silver_sales_inconsistency ON COMMIT DROP AS
  SELECT DISTINCT sls_sales, sls_quantity, sls_price
  FROM silver.crm_sales_details
  WHERE sls_sales <> sls_quantity * sls_price
     OR sls_sales IS NULL
     OR sls_quantity IS NULL
     OR sls_price IS NULL
     OR sls_sales <= 0
     OR sls_quantity <= 0
     OR sls_price <= 0;

  SELECT COUNT(*) INTO n_sales_incons FROM tmp_silver_sales_inconsistency;
  IF n_sales_incons = 0 THEN
    RAISE NOTICE '[OK] crm_sales_details: sales = qty*price & values > 0 ✓';
  ELSE
    RAISE NOTICE '[FAIL] crm_sales_details: % row(s) inconsistent. See tmp_silver_sales_inconsistency.', n_sales_incons;
  END IF;

  ------------------------------------------------------------------
  -- 4) silver.erp_cust_az12
  ------------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_silver_bdate_out_of_range;
  CREATE TEMP TABLE tmp_silver_bdate_out_of_range ON COMMIT DROP AS
  SELECT DISTINCT bdate
  FROM silver.erp_cust_az12
  WHERE bdate < DATE '1924-01-01'
     OR bdate > CURRENT_DATE;

  SELECT COUNT(*) INTO n_bdate_oor FROM tmp_silver_bdate_out_of_range;
  IF n_bdate_oor = 0 THEN
    RAISE NOTICE '[OK] erp_cust_az12: birthdates in valid range ✓';
  ELSE
    RAISE NOTICE '[FAIL] erp_cust_az12: % out-of-range birthdate(s). See tmp_silver_bdate_out_of_range.', n_bdate_oor;
  END IF;

  -- (enum) SELECT DISTINCT gen ... (run manually if needed)

  ------------------------------------------------------------------
  -- 5) silver.erp_loc_a101
  ------------------------------------------------------------------
  -- (enum) SELECT DISTINCT cntry ... (run manually if needed)

  ------------------------------------------------------------------
  -- 6) silver.erp_px_cat_g1v2
  ------------------------------------------------------------------
  DROP TABLE IF EXISTS tmp_silver_pxcat_spaces;
  CREATE TEMP TABLE tmp_silver_pxcat_spaces ON COMMIT DROP AS
  SELECT *
  FROM silver.erp_px_cat_g1v2
  WHERE cat <> TRIM(cat)
     OR subcat <> TRIM(subcat)
     OR maintenance <> TRIM(maintenance);

  SELECT COUNT(*) INTO n_pxcat_spaces FROM tmp_silver_pxcat_spaces;
  IF n_pxcat_spaces = 0 THEN
    RAISE NOTICE '[OK] erp_px_cat_g1v2: no unwanted spaces in text cols ✓';
  ELSE
    RAISE NOTICE '[FAIL] erp_px_cat_g1v2: % row(s) with unwanted spaces. See tmp_silver_pxcat_spaces.', n_pxcat_spaces;
  END IF;

  ------------------------------------------------------------------
  -- Summary
  ------------------------------------------------------------------
  RAISE NOTICE '================ Summary ========================';
  RAISE NOTICE 'crm_cust_info: dup/null cst_id     = %', n_dup_cst_id;
  RAISE NOTICE 'crm_cust_info: spaced keys         = %', n_spaces_cst_key;

  RAISE NOTICE 'crm_prd_info : dup/null prd_id     = %', n_dup_prd_id;
  RAISE NOTICE 'crm_prd_info : spaced names        = %', n_spaces_prd_nm;
  RAISE NOTICE 'crm_prd_info : bad costs           = %', n_bad_prd_cost;
  RAISE NOTICE 'crm_prd_info : end<start rows      = %', n_prd_date_order;

  RAISE NOTICE 'sales_details: invalid raw dates   = %', n_invalid_raw_dt;
  RAISE NOTICE 'sales_details: bad date order      = %', n_sales_date_ord;
  RAISE NOTICE 'sales_details: sales inconsist     = %', n_sales_incons;

  RAISE NOTICE 'erp_cust_az12: bdate out-of-range  = %', n_bdate_oor;
  RAISE NOTICE 'px_cat_g1v2  : text with spaces    = %', n_pxcat_spaces;
  RAISE NOTICE '==================================================';

  -- Optional: fail pipeline if any issue found
  -- IF (n_dup_cst_id + n_spaces_cst_key + n_dup_prd_id + n_spaces_prd_nm
  --     + n_bad_prd_cost + n_prd_date_order + n_invalid_raw_dt
  --     + n_sales_date_ord + n_sales_incons + n_bdate_oor + n_pxcat_spaces) > 0 THEN
  --   RAISE EXCEPTION 'Silver validation failed. See tmp_* tables for details.';
  -- END IF;

END;
$$;

-- Usage:
-- CALL silver.validate_silver();
-- Then inspect any temp tables with rows, e.g.:
-- SELECT * FROM tmp_silver_dup_cst_id;
-- SELECT * FROM tmp_silver_spaces_cst_key;
-- SELECT * FROM tmp_silver_dup_prd_id;
-- SELECT * FROM tmp_silver_spaces_prd_nm;
-- SELECT * FROM tmp_silver_bad_prd_cost;
-- SELECT * FROM tmp_silver_prd_bad_date_order;
-- SELECT * FROM tmp_silver_invalid_bronze_dates;
-- SELECT * FROM tmp_silver_sales_bad_date_order;
-- SELECT * FROM tmp_silver_sales_inconsistency;
-- SELECT * FROM tmp_silver_bdate_out_of_range;
-- SELECT * FROM tmp_silver_pxcat_spaces;
