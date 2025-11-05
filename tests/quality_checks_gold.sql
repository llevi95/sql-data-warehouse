CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE PROCEDURE gold.validate_gold()
LANGUAGE plpgsql
AS $$
/*
Purpose:
  Run GOLD-layer data quality checks and summarize results.

What it does:
  1) Checks surrogate key uniqueness in gold.dim_customers and gold.dim_products
  2) Checks referential integrity between gold.fact_sales and those dimensions
  3) Writes violating rows (if any) into TEMP tables:
       - tmp_gold_dup_dim_customers
       - tmp_gold_dup_dim_products
       - tmp_gold_fact_missing_dim
     (All ON COMMIT DROP; visible only in the current session)
*/
DECLARE
  n_dup_cust   bigint := 0;
  n_dup_prod   bigint := 0;
  n_fk_breaks  bigint := 0;
BEGIN
  RAISE NOTICE '================ GOLD Validation ================';

  -- 1) Uniqueness: gold.dim_customers.customer_key
  BEGIN
    DROP TABLE IF EXISTS tmp_gold_dup_dim_customers;
    CREATE TEMP TABLE tmp_gold_dup_dim_customers ON COMMIT DROP AS
    SELECT customer_key, COUNT(*) AS duplicate_count
    FROM gold.dim_customers
    GROUP BY customer_key
    HAVING COUNT(*) > 1;

    SELECT COUNT(*) INTO n_dup_cust FROM tmp_gold_dup_dim_customers;
    IF n_dup_cust = 0 THEN
      RAISE NOTICE '[OK] dim_customers: customer_key uniqueness ✓';
    ELSE
      RAISE NOTICE '[FAIL] dim_customers: % duplicate key value(s). See tmp_gold_dup_dim_customers.', n_dup_cust;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '[ERROR] Checking dim_customers: %', SQLERRM;
    RAISE;
  END;

  -- 2) Uniqueness: gold.dim_products.product_key
  BEGIN
    DROP TABLE IF EXISTS tmp_gold_dup_dim_products;
    CREATE TEMP TABLE tmp_gold_dup_dim_products ON COMMIT DROP AS
    SELECT product_key, COUNT(*) AS duplicate_count
    FROM gold.dim_products
    GROUP BY product_key
    HAVING COUNT(*) > 1;

    SELECT COUNT(*) INTO n_dup_prod FROM tmp_gold_dup_dim_products;
    IF n_dup_prod = 0 THEN
      RAISE NOTICE '[OK] dim_products: product_key uniqueness ✓';
    ELSE
      RAISE NOTICE '[FAIL] dim_products: % duplicate key value(s). See tmp_gold_dup_dim_products.', n_dup_prod;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '[ERROR] Checking dim_products: %', SQLERRM;
    RAISE;
  END;

  -- 3) Referential integrity: fact_sales → dims
  BEGIN
    DROP TABLE IF EXISTS tmp_gold_fact_missing_dim;
    CREATE TEMP TABLE tmp_gold_fact_missing_dim ON COMMIT DROP AS
    SELECT
      f.order_number,
      f.product_key  AS fact_product_key,
      p.product_key  AS dim_product_key,
      f.customer_key AS fact_customer_key,
      c.customer_key AS dim_customer_key
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products  p ON f.product_key  = p.product_key
    LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
    WHERE p.product_key IS NULL OR c.customer_key IS NULL;

    SELECT COUNT(*) INTO n_fk_breaks FROM tmp_gold_fact_missing_dim;
    IF n_fk_breaks = 0 THEN
      RAISE NOTICE '[OK] fact_sales: referential integrity to dims ✓';
    ELSE
      RAISE NOTICE '[FAIL] fact_sales: % broken references. See tmp_gold_fact_missing_dim.', n_fk_breaks;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '[ERROR] Checking fact_sales relationships: %', SQLERRM;
    RAISE;
  END;

  RAISE NOTICE '================ Summary ========================';
  RAISE NOTICE 'Duplicates in dim_customers: %', n_dup_cust;
  RAISE NOTICE 'Duplicates in dim_products : %', n_dup_prod;
  RAISE NOTICE 'Broken fact→dim refs      : %', n_fk_breaks;
  RAISE NOTICE '==================================================';
  IF n_dup_cust > 0 THEN
    RAISE NOTICE 'Inspect: SELECT * FROM tmp_gold_dup_dim_customers;';
  END IF;
  IF n_dup_prod > 0 THEN
    RAISE NOTICE 'Inspect: SELECT * FROM tmp_gold_dup_dim_products;';
  END IF;
  IF n_fk_breaks > 0 THEN
    RAISE NOTICE 'Inspect: SELECT * FROM tmp_gold_fact_missing_dim;';
  END IF;
END;
$$;

-- Usage:
-- CALL gold.validate_gold();
-- (then optionally:)
-- SELECT * FROM tmp_gold_dup_dim_customers;
-- SELECT * FROM tmp_gold_dup_dim_products;
-- SELECT * FROM tmp_gold_fact_missing_dim;