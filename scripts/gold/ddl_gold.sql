/*
===============================================================================
Script: Build Gold Layer (Silver → Gold)
===============================================================================
Script Purpose:
    This script constructs the analytical (Gold) layer of the data warehouse.
    It defines *views* that represent the business-ready entities used for
    reporting and analytics.

    The Gold layer is derived entirely from the Silver layer and consists of:
      • Dimension Views (gold.dim_customers, gold.dim_products)
      • Fact View (gold.fact_sales)

    Each view performs data integration and enrichment through joins and
    transformations, following a star schema design.

Core Logic:
    - Drops existing views in dependency order (fact → dimensions).
    - Recreates the following views:
        1. gold.dim_customers
        2. gold.dim_products
        3. gold.fact_sales

View Details:
    1️⃣ gold.dim_customers
        • Combines CRM and ERP customer data.
        • Derives a surrogate `customer_key` using ROW_NUMBER().
        • Joins:
            - silver.crm_cust_info  (base customer data)
            - silver.erp_cust_az12  (birthdate, gender)
            - silver.erp_loc_a101   (country)
        • Normalizes gender (CRM preferred, ERP fallback).
        • Includes demographic and creation metadata.

    2️⃣ gold.dim_products
        • Integrates CRM product data with ERP category mappings.
        • Creates surrogate `product_key` via ROW_NUMBER().
        • Joins:
            - silver.crm_prd_info   (product master)
            - silver.erp_px_cat_g1v2 (category/subcategory details)
        • Includes category, subcategory, maintenance info, and product line.
        • Filters to only include current (non-historical) products where
          `prd_end_dt IS NULL`.

    3️⃣ gold.fact_sales
        • Represents the central fact table for sales analysis.
        • Joins sales transactions with both dimensions:
            - gold.dim_products
            - gold.dim_customers
        • Provides business metrics:
            order_number, order_date, shipping_date, due_date, sales_amount,
            quantity, and price.
        • Maintains referential linkage via surrogate keys from dimensions.

Schema Flow:
    BRONZE → SILVER → GOLD (views)

Dependencies:
    - All Silver layer tables must exist and be populated before executing this script.
    - The 'gold' schema is created automatically if not present.

Execution Steps:
    1. Ensure the Silver layer is fully loaded.
    2. Run this script in your SQL environment:

Output:
    - Analytical views ready for BI/reporting consumption.
    - No physical data stored — all views resolve dynamically from Silver.

Error Handling:
    - Existing views are dropped in dependency order to prevent conflicts.
    - No exception handling required since this is DDL-only.

Idempotency:
    - Safe to re-run; views are recreated each time.

===============================================================================
*/

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS gold;

-- Drop in dependency order (fact → dims)
DROP VIEW IF EXISTS gold.fact_sales;
DROP VIEW IF EXISTS gold.dim_products;
DROP VIEW IF EXISTS gold.dim_customers;

-- =============================================================================
-- gold.dim_customers
-- =============================================================================
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,  -- surrogate key (volatile in a view)
    ci.cst_id            AS customer_id,
    ci.cst_key           AS customer_number,
    ci.cst_firstname     AS first_name,
    ci.cst_lastname      AS last_name,
    la.cntry             AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr         -- CRM primary for gender
        ELSE COALESCE(ca.gen, 'n/a')                       -- fallback to ERP
    END                 AS gender,
    ca.bdate            AS birthdate,
    ci.cst_create_date  AS create_date
FROM silver.crm_cust_info  AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
  ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
  ON ci.cst_key = la.cid;

-- =============================================================================
-- gold.dim_products
-- =============================================================================
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
  ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;  -- current (non-historical) rows

-- =============================================================================
-- gold.fact_sales
-- =============================================================================
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products  AS pr
  ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
  ON sd.sls_cust_id = cu.customer_id;
