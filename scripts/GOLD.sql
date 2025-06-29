
-- Description:   This script creates dimensional views in the [Gold] schema of the 
--                [DataWareHouse] database by transforming and joining raw source data 
--                from the [Silver] schema. These views are designed to be used in 
--                analytical workloads such as reporting and dashboards.
-- 
-- Views Created:
--   1. [Gold].[dim_customers]   - Customer dimension
--   2. [Gold].[dim_products]    - Product dimension
--   3. [Gold].[fact_sales]  - Sales fact table with links to product and customer dims
-- ========================================================================================


-- ========================================================================================
-- DROP AND CREATE: [Gold].[dim_customers]
-- Description: Creates a customer dimension view that combines customer info, birth date, 
--              and country from multiple Silver schema tables. Generates a surrogate key 
--              using ROW_NUMBER().
-- ========================================================================================
USE [DataWareHouse]
GO

IF OBJECT_ID('[Gold].[dim_customers]', 'V') IS NOT NULL 
    DROP VIEW [Gold].[dim_customers]
GO

CREATE VIEW [Gold].[dim_customers] AS
SELECT
    ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key, -- Surrogate key
    ci.cst_id AS customer_id,                              -- Natural customer ID
    ci.cst_key AS customer_number,                         -- External system customer number
    CONCAT(ci.cst_firstname, ' ', ci.cst_lastname) AS customer_name,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'N/A')
    END AS gender,                                         -- Gender fallback logic
    ci.cst_create_date AS create_date,
    ca.bdate AS birth_date,
    la.country AS cust_country                             -- Joined from location table
FROM Silver.crm_cust_info AS ci
LEFT JOIN Silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
LEFT JOIN Silver.erp_loc_101 AS la ON ci.cst_key = la.cid



-- ========================================================================================
-- DROP AND CREATE: [Gold].[dim_products]
-- Description: Creates a product dimension view by joining product info with its 
--              category details. Filters out products that have ended (prd_end_dt IS NULL).
-- ========================================================================================
USE [DataWareHouse]
GO

IF OBJECT_ID('Gold.dim_products', 'V') IS NOT NULL 
    DROP VIEW Gold.dim_products
GO

CREATE VIEW Gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, prd_key) AS product_key, -- Surrogate key
    pn.prd_id AS product_id,
    pn.prd_key AS prd_key_number,
    pn.prd_cat_id AS prd_subkey_number,
    pn.sales_prd_key AS sales_prd_key, 
    pn.prd_nm AS prd_number,
    pc.cat AS prd_cat,
    pc.maintanance AS prd_maintanance,
    pn.prd_cost AS prd_cost,
    pn.prd_line AS prd_line_type,
    pn.prd_start_dt AS prd_start_dt
FROM Silver.crm_prd_info AS pn
LEFT JOIN Silver.erp_px_cat_g1v2 AS pc ON pn.prd_cat_id = pc.id
WHERE pn.prd_end_dt IS NULL



-- ========================================================================================
-- DROP AND CREATE: [Gold].[dim_fact_sales]
-- Description: Creates a sales fact view that joins sales transactions with dimensional 
--              data for customers and products. It includes order details and financial 
--              metrics like sales amount and price.
-- ========================================================================================
USE [DataWareHouse]
GO

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL 
    DROP VIEW gold.fact_sales
GO

CREATE VIEW gold.fact_sales AS
SELECT 
    cs.sls_ord_num AS order_number,
    pr.product_key AS product_key,         -- FK to product dimension
    dc.customer_key AS customer_key,       -- FK to customer dimension
    cs.sls_order_dt AS order_date,
    cs.sls_ship_dt AS ship_date,
    cs.sls_due_dt AS due_date,
    cs.sls_sales AS total_sales,
    cs.sls_quantity AS sales_quantity,
    cs.sls_price AS sale_price
FROM Silver.crm_sales_info AS cs
LEFT JOIN gold.dim_products AS pr ON pr.sales_prd_key = cs.sls_prd_key
LEFT JOIN gold.dim_customers AS dc ON dc.customer_id = cs.sls_cust_id
