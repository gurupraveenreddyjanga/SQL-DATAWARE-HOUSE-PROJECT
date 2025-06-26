### Summary

Created a stored procedure `silver.load_silver` that performs ETL (Extract, Transform, Load) operations from the Bronze layer to the Silver layer. This procedure includes data cleansing, transformation, and enrichment logic across several CRM and ERP tables.

### Key Features

- **Truncation and Load** for the following Silver tables:
  - `Silver.crm_cust_info`
  - `Silver.crm_prd_info`
  - `Silver.crm_sales_info`
  - `Silver.erp_cust_az12`
  - `Silver.erp_loc_101`
  - `Silver.erp_px_cat_g1v2`

- **Transformations include:**
  - Standardizing gender and marital status values
  - Removing invalid birth dates
  - Deriving category and sales keys from product key
  - Computing product end dates using `LEAD()`
  - Handling nulls and invalid date fields
  - Ensuring deduplication using `ROW_NUMBER()`

- **Performance tracking:**
  - Per-table duration printed using `GETDATE()` and `DATEDIFF`
  - Total execution time calculated and printed

- **Error handling:**
  - TRY...CATCH block captures and prints any runtime SQL errors
  - Error message includes line number, severity, and state

### Improvements

- Added `PRINT` statements to clearly mark the start and completion of each table load
- Ensured `EXEC silver.load_silver` is **not inside** the procedure to prevent infinite recursion
- Added total run-time tracking for better performance insights

### Notes

- This is a critical part of the ETL pipeline that prepares data for downstream analytics/reporting.
- Make sure to execute the procedure separately using `EXEC silver.load_silver` **after deployment**, not within the body of the procedure.




CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @proc_start_time DATETIME = GETDATE();
    DECLARE @table_start_time DATETIME;
    DECLARE @elapsed_seconds INT;

    BEGIN TRY

    -- ===============================================
    -- Process: Customer Information (CRM)
    -- ===============================================
    SET @table_start_time = GETDATE();
    PRINT '--- Starting load for table: Silver.crm_cust_info'

    TRUNCATE TABLE Silver.crm_cust_info

    INSERT INTO Silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE UPPER(cst_marital_status)
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE 'N/A'
        END,
        CASE UPPER(cst_gndr)
            WHEN 'F' THEN 'Female'
            WHEN 'M' THEN 'Male'
            ELSE 'N/A'
        END,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_cst_id
        FROM Bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_cst_id = 1

    SET @elapsed_seconds = DATEDIFF(SECOND, @table_start_time, GETDATE());
    PRINT 'Completed load for Silver.crm_cust_info in ' + CAST(@elapsed_seconds AS VARCHAR) + ' seconds'


    -- ===============================================
    -- Process: Product Information (CRM)
    -- ===============================================
    SET @table_start_time = GETDATE();
    PRINT '--- Starting load for table: Silver.crm_prd_info'

    TRUNCATE TABLE Silver.crm_prd_info

    INSERT INTO Silver.crm_prd_info (
        prd_id, prd_key, prd_cat_id, sales_prd_key,
        prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        prd_key,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7, LEN(prd_key)),
        prd_nm,
        COALESCE(prd_cost, 0),
        COALESCE(prd_line, 'N/A'),
        CAST(prd_start_dt AS DATE),
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE)
    FROM Bronze.crm_prd_info

    SET @elapsed_seconds = DATEDIFF(SECOND, @table_start_time, GETDATE());
    PRINT 'Completed load for Silver.crm_prd_info in ' + CAST(@elapsed_seconds AS VARCHAR) + ' seconds'


    -- ===============================================
    -- Process: Sales Information (CRM)
    -- ===============================================
    SET @table_start_time = GETDATE();
    PRINT '--- Starting load for table: Silver.crm_sales_info'

    TRUNCATE TABLE Silver.crm_sales_info

    INSERT INTO Silver.crm_sales_info (
        sls_ord_num, sls_prd_key, sls_cust_id, 
        sls_order_dt, sls_ship_dt, sls_due_dt, 
        sls_quantity, sls_price, sls_sales
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
        END,
        CASE 
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
        END,
        CASE 
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
        END,
        sls_quantity,
        CASE 
            WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / sls_quantity 
            ELSE sls_price 
        END,
        sls_quantity * (
            CASE 
                WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / sls_quantity 
                ELSE sls_price 
            END
        )
    FROM Bronze.crm_sales_info

    SET @elapsed_seconds = DATEDIFF(SECOND, @table_start_time, GETDATE());
    PRINT 'Completed load for Silver.crm_sales_info in ' + CAST(@elapsed_seconds AS VARCHAR) + ' seconds'


    -- ===============================================
    -- ERP Customer Info (AZ12)
    -- ===============================================
    SET @table_start_time = GETDATE();
    PRINT '--- Starting load for table: Silver.erp_cust_az12'

    TRUNCATE TABLE Silver.erp_cust_az12

    INSERT INTO Silver.erp_cust_az12 (
        cid, bdate, gen
    )
    SELECT
        REPLACE(cid, 'NAS', ''),
        CASE 
            WHEN bdate > GETDATE() OR bdate < '1924-01-01' THEN NULL 
            ELSE bdate 
        END,
        CASE 
            WHEN UPPER(gen) = 'F' THEN 'Female'
            WHEN UPPER(gen) = 'M' THEN 'Male'
            WHEN LEN(TRIM(gen)) = 0 OR gen IS NULL THEN 'N/A'
            ELSE TRIM(gen) 
        END
    FROM Bronze.erp_cust_az12

    SET @elapsed_seconds = DATEDIFF(SECOND, @table_start_time, GETDATE());
    PRINT 'Completed load for Silver.erp_cust_az12 in ' + CAST(@elapsed_seconds AS VARCHAR) + ' seconds'


    -- ===============================================
    -- ERP Location Info
    -- ===============================================
    SET @table_start_time = GETDATE();
    PRINT '--- Starting load for table: Silver.erp_loc_101'

    TRUNCATE TABLE Silver.erp_loc_101

    INSERT INTO Silver.erp_loc_101 (
        cid, country
    )
    SELECT
        REPLACE(TRIM(cid), '-', ''),
        CASE 
            WHEN TRIM(country) = 'DE' THEN 'Germany'
            WHEN TRIM(country) IN ('US', 'USA') THEN 'United States'
            WHEN country IS NULL OR TRIM(country) = '' THEN 'N/A'
            ELSE TRIM(country) 
        END
    FROM Bronze.erp_loc_101

    SET @elapsed_seconds = DATEDIFF(SECOND, @table_start_time, GETDATE());
    PRINT 'Completed load for Silver.erp_loc_101 in ' + CAST(@elapsed_seconds AS VARCHAR) + ' seconds'


    -- ===============================================
    -- ERP Product Category Info
    -- ===============================================
    SET @table_start_time = GETDATE();
    PRINT '--- Starting load for table: Silver.erp_px_cat_g1v2'

    TRUNCATE TABLE Silver.erp_px_cat_g1v2

    INSERT INTO Silver.erp_px_cat_g1v2 (
        id, cat, subcat, maintanance
    )
    SELECT 
        id, cat, subcat, maintanance
    FROM Bronze.erp_px_cat_g1v2

    SET @elapsed_seconds = DATEDIFF(SECOND, @table_start_time, GETDATE());
    PRINT 'Completed load for Silver.erp_px_cat_g1v2 in ' + CAST(@elapsed_seconds AS VARCHAR) + ' seconds'


    -- ===============================================
    -- Final Success Message
    -- ===============================================
    DECLARE @proc_end_time DATETIME = GETDATE();
    SET @elapsed_seconds = DATEDIFF(SECOND, @proc_start_time, @proc_end_time);
    PRINT '=== All tables loaded successfully into Silver layer ===';
    PRINT '=== Total time taken: ' + CAST(@elapsed_seconds AS VARCHAR) + ' seconds ==='

    END TRY

    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrLine INT = ERROR_LINE();
        DECLARE @ErrSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrState INT = ERROR_STATE();
        PRINT '*** ERROR OCCURRED ***'
        PRINT 'Message: ' + @ErrMsg
        PRINT 'Line: ' + CAST(@ErrLine AS VARCHAR)
        PRINT 'Severity: ' + CAST(@ErrSeverity AS VARCHAR) + ' State: ' + CAST(@ErrState AS VARCHAR)
    END CATCH
END

GO
-- Execute the procedure
EXEC silver.load_silver
