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