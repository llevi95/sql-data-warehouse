/*
===============================================================================
Stored Procedure: Rebuild & Load Bronze Layer (CSV -> bronze)
===============================================================================
Script Purpose:
    Rebuilds the entire 'bronze' layer from external CSV files.
    For each bronze table, the procedure:
      1) DROPs the table if it exists,
      2) CREATEs the table with the expected structure,
      3) COPY-loads data from the specified CSV file.

What this means:
    - This is a destructive operation. Existing bronze tables (and their data)
      are removed and recreated on each run.

Tables & Sources:
    - bronze.crm_cust_info      <- datasets/source_crm/cust_info.csv
    - bronze.crm_prd_info       <- datasets/source_crm/prd_info.csv
    - bronze.crm_sales_details  <- datasets/source_crm/sales_details.csv
    - bronze.erp_loc_a101       <- datasets/source_erp/LOC_A101.csv
    - bronze.erp_cust_az12      <- datasets/source_erp/CUST_AZ12.csv
    - bronze.erp_px_cat_g1v2    <- datasets/source_erp/PX_CAT_G1V2.csv

Parameters:
    None.
    This stored procedure does not accept parameters and does not return values.

Usage:
    CALL bronze.rebuild_and_load_bronze();

Progress & Timing:
    - Emits NOTICE messages per table with start/end and elapsed time.

Prerequisites:
    - Schema 'bronze' exists (the script ensures this with CREATE SCHEMA IF NOT EXISTS).
    - The PostgreSQL server process must be able to read the CSV files at the
      given absolute paths used by COPY.
      * On macOS/local dev, server-side COPY needs file permissions for the
        postgres OS user. If that’s not feasible, run client-side \copy in psql.
    - CSVs are comma-delimited with a header row.

Idempotency:
    - Safe to re-run: tables are dropped and recreated each run.

Failure Behavior:
    - On error, the exception is surfaced; the procedure prints a descriptive
      NOTICE with SQLERRM and re-raises the error.

DON'T FORGET:
    - The current script uses absolute paths.
      Ensure these paths are valid and readable by the PostgreSQL server host (switch 'PATH' for yours).
===============================================================================
*/
-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS bronze;

-- End-to-end BRONZE rebuild & load (DROP -> CREATE -> COPY)
CREATE OR REPLACE PROCEDURE bronze.rebuild_and_load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time timestamp;
    v_end_time   timestamp;
BEGIN
    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Rebuild & Load: BRONZE layer (drop, create, copy)';
    RAISE NOTICE '===============================================';

    ----------------------------------------------------------------------
    -- bronze.crm_cust_info
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate bronze.crm_cust_info';
    DROP TABLE IF EXISTS bronze.crm_cust_info;
    CREATE TABLE bronze.crm_cust_info (
        cst_id             INTEGER,
        cst_key            VARCHAR(50),
        cst_firstname      VARCHAR(50),
        cst_lastname       VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr           VARCHAR(50),
        cst_create_date    DATE
    );
    RAISE NOTICE '>> COPY bronze.crm_cust_info';
    COPY bronze.crm_cust_info
      FROM 'PATH'
      WITH (FORMAT csv, HEADER true, DELIMITER ',');
    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- bronze.crm_prd_info
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate bronze.crm_prd_info';
    DROP TABLE IF EXISTS bronze.crm_prd_info;
    CREATE TABLE bronze.crm_prd_info (
        prd_id       INTEGER,
        prd_key      VARCHAR(50),
        prd_nm       VARCHAR(50),
        prd_cost     INTEGER,
        prd_line     VARCHAR(50),
        prd_start_dt DATE,
		prd_end_dt	DATE
    );
    RAISE NOTICE '>> COPY bronze.crm_prd_info';
    COPY bronze.crm_prd_info
      FROM 'PATH'
      WITH (FORMAT csv, HEADER true, DELIMITER ',');
    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- bronze.crm_sales_details
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate bronze.crm_sales_details';
    DROP TABLE IF EXISTS bronze.crm_sales_details;
    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num  VARCHAR(50),
        sls_prd_key  VARCHAR(50),
        sls_cust_id  INTEGER,
        sls_order_dt INTEGER,  -- source as YYYYMMDD integer
        sls_ship_dt  INTEGER,
        sls_due_dt   INTEGER,
        sls_sales    INTEGER,
        sls_quantity INTEGER,
        sls_price    INTEGER
    );
    RAISE NOTICE '>> COPY bronze.crm_sales_details';
    COPY bronze.crm_sales_details
      FROM 'PATH'
      WITH (FORMAT csv, HEADER true, DELIMITER ',');
    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- bronze.erp_loc_a101
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate bronze.erp_loc_a101';
    DROP TABLE IF EXISTS bronze.erp_loc_a101;
    CREATE TABLE bronze.erp_loc_a101 (
        cid   VARCHAR(50),
        cntry VARCHAR(50)
    );
    RAISE NOTICE '>> COPY bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
      FROM 'PATH'
      WITH (FORMAT csv, HEADER true, DELIMITER ',');
    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- bronze.erp_cust_az12
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate bronze.erp_cust_az12';
    DROP TABLE IF EXISTS bronze.erp_cust_az12;
    CREATE TABLE bronze.erp_cust_az12 (
        cid   VARCHAR(50),
        bdate DATE,
        gen   VARCHAR(50)
    );
    RAISE NOTICE '>> COPY bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
      FROM 'PATH'
      WITH (FORMAT csv, HEADER true, DELIMITER ',');
    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    ----------------------------------------------------------------------
    -- bronze.erp_px_cat_g1v2
    ----------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Recreate bronze.erp_px_cat_g1v2';
    DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
    CREATE TABLE bronze.erp_px_cat_g1v2 (
        id          VARCHAR(50),
        cat         VARCHAR(50),
        subcat      VARCHAR(50),
        maintenance VARCHAR(50)
    );
    RAISE NOTICE '>> COPY bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
      FROM 'PATH'
      WITH (FORMAT csv, HEADER true, DELIMITER ',');
    v_end_time := clock_timestamp();
    RAISE NOTICE '   done in %s', (v_end_time - v_start_time);

    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Bronze layer rebuild & load completed';
    RAISE NOTICE '===============================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR during Bronze rebuild & load: %', SQLERRM;
    RAISE;
END;
$$;

-- Usage:
-- CALL bronze.rebuild_and_load_bronze();
