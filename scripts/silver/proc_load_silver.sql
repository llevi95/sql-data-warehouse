/*
===============================================================================
Stored Procedure: Rebuild & Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure rebuilds and populates the 'silver' schema
    by transforming, cleansing, and enriching data from the 'bronze' layer.
    For each table in the silver layer, the procedure:
      1) DROPs the table if it exists,
      2) CREATEs it with the defined structure,
      3) INSERTs transformed data from the corresponding bronze table.

Transformation Logic Summary:
    • silver.crm_cust_info:
        - Keeps the most recent record per customer (ROW_NUMBER window).
        - Normalizes gender and marital status codes.
        - Trims extra whitespace from text fields.
    • silver.crm_prd_info:
        - Derives cat_id from product key (first 5 chars).
        - Replaces product line codes (M → Mountain, R → Road, etc.).
        - Calculates prd_end_dt using LEAD() over prd_start_dt.
    • silver.crm_sales_details:
        - Converts integer date formats (YYYYMMDD) to DATE.
        - Validates sales and price consistency.
        - Auto-calculates missing or invalid sales values.
    • silver.erp_loc_a101:
        - Standardizes country names (DE → Germany, US/USA → United States).
        - Cleans empty or null values to 'n/a'.
    • silver.erp_cust_az12:
        - Cleans up customer IDs (removes 'NAS' prefix).
        - Nullifies invalid future birthdates.
        - Normalizes gender labels (F/M → Female/Male).
    • silver.erp_px_cat_g1v2:
        - Straight copy from bronze with added dwh_create_date.

Parameters:
    None.
    This stored procedure does not accept parameters or return values.

Usage:
    CALL silver.rebuild_and_load_silver();

Execution Flow:
    - Prints NOTICE messages per table with timing (start/end/duration).
    - Can be safely re-run — each run rebuilds the silver tables from scratch.

Dependencies:
    - Requires the bronze schema to be fully loaded (use `bronze.rebuild_and_load_bronze()` first).
    - Each bronze table must exist and contain data before this procedure runs.

Output:
    - All silver tables freshly created and populated.
    - Each record receives a 'dwh_create_date' timestamp at insert time.

Error Handling:
    - Captures and logs SQL errors via NOTICE messages.
    - Re-raises the error after reporting for visibility.

Notes:
    - Ensure the 'silver' schema exists (created automatically if missing).
    - No file system access needed: all data transformations happen within PostgreSQL.
===============================================================================
*/
CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE PROCEDURE silver.rebuild_and_load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time timestamp;
    v_end_time   timestamp;
BEGIN
    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Rebuild & Load: SILVER layer (drop, create, insert)';
    RAISE NOTICE '===============================================';

    ----------------------------------------------------------------------
    -- silver.crm_cust_info
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate silver.crm_cust_info';
    DROP TABLE IF EXISTS silver.crm_cust_info;

    CREATE TABLE silver.crm_cust_info (
        cst_id             INTEGER,
        cst_key            VARCHAR(50),
        cst_firstname      VARCHAR(50),
        cst_lastname       VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr           VARCHAR(50),
        cst_create_date    DATE,
        dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE '>> Insert silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- silver.crm_prd_info
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate silver.crm_prd_info';
    DROP TABLE IF EXISTS silver.crm_prd_info;

    CREATE TABLE silver.crm_prd_info (
        prd_id          INTEGER,
        cat_id          VARCHAR(50),
        prd_key         VARCHAR(50),
        prd_nm          VARCHAR(50),
        prd_cost        INTEGER,
        prd_line        VARCHAR(50),
        prd_start_dt    DATE,
        prd_end_dt      DATE,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE '>> Insert silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key FROM 7)                          AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0)                              AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END                                                AS prd_line,
        prd_start_dt::date                                 AS prd_start_dt,
        (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
            - INTERVAL '1 day')::date                      AS prd_end_dt
    FROM bronze.crm_prd_info;

    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- silver.crm_sales_details
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate silver.crm_sales_details';
    DROP TABLE IF EXISTS silver.crm_sales_details;

    CREATE TABLE silver.crm_sales_details (
        sls_ord_num     VARCHAR(50),
        sls_prd_key     VARCHAR(50),
        sls_cust_id     INTEGER,
        sls_order_dt    DATE,
        sls_ship_dt     DATE,
        sls_due_dt      DATE,
        sls_sales       INTEGER,
        sls_quantity    INTEGER,
        sls_price       INTEGER,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE '>> Insert silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 OR length(sls_order_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR length(sls_ship_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 OR length(sls_due_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales <> sls_quantity * abs(sls_price) 
            THEN sls_quantity * abs(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
            THEN (sls_sales::numeric / NULLIF(sls_quantity, 0))
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- silver.erp_loc_a101
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate silver.erp_loc_a101';
    DROP TABLE IF EXISTS silver.erp_loc_a101;

    CREATE TABLE silver.erp_loc_a101 (
        cid             VARCHAR(50),
        cntry           VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE '>> Insert silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid, 
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- silver.erp_cust_az12
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate silver.erp_cust_az12';
    DROP TABLE IF EXISTS silver.erp_cust_az12;

    CREATE TABLE silver.erp_cust_az12 (
        cid             VARCHAR(50),
        bdate           DATE,
        gen             VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE '>> Insert silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
            ELSE cid
        END AS cid, 
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- silver.erp_px_cat_g1v2
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate silver.erp_px_cat_g1v2';
    DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

    CREATE TABLE silver.erp_px_cat_g1v2 (
        id              VARCHAR(50),
        cat             VARCHAR(50),
        subcat          VARCHAR(50),
        maintenance     VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    RAISE NOTICE '>> Insert silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Silver layer rebuild & load completed';
    RAISE NOTICE '===============================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR during Silver rebuild & load: %', SQLERRM;
    RAISE;
END;
$$;

-- Run it:
-- CALL silver.rebuild_and_load_silver();
