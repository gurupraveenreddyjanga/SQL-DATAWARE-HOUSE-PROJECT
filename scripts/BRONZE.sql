  -- =============================================
-- Project: Data Warehouse - Bronze Layer Loader
-- Author: GURU PRAVEEN REDDY J
-- Purpose: Create bronze layer tables and load data from raw .csv files
-- Last Updated: 2025-06-26
-- =============================================

-- Use the DataWarehouse database
USE DataWareHouse;

-- Drop and recreate Bronze.crm_cust_info table if exists
IF OBJECT_ID('Bronze.crm_cust_info','U') IS NOT NULL DROP TABLE Bronze.crm_cust_info;

CREATE TABLE Bronze.crm_cust_info(
    cst_id INT,                          -- Customer ID
    cst_key NVARCHAR(25),                -- Customer unique key
    cst_firstname NVARCHAR(25),          -- First name
    cst_lastname NVARCHAR(25),           -- Last name
    cst_marital_status NVARCHAR(25),     -- Marital status
    cst_gndr NVARCHAR(25),               -- Gender
    cst_create_date DATE                 -- Date of creation
);

-- Drop and recreate Bronze.crm_prd_info table if exists
IF OBJECT_ID('Bronze.crm_prd_info','U') IS NOT NULL DROP TABLE Bronze.crm_prd_info;

CREATE TABLE Bronze.crm_prd_info(
    prd_id INT,                          -- Product ID
    prd_key NVARCHAR(50),                -- Product key
    prd_nm NVARCHAR(50),                 -- Product name
    prd_cost INT,                        -- Cost
    prd_line NVARCHAR(50),               -- Product line/category
    prd_start_dt DATETIME,              -- Start date
    prd_end_dt DATETIME                 -- End date
);

-- Drop and recreate Bronze.crm_sales_info table if exists
IF OBJECT_ID('Bronze.crm_sales_info','U') IS NOT NULL DROP TABLE Bronze.crm_sales_info;

CREATE TABLE Bronze.crm_sales_info(
    sls_ord_num NVARCHAR(50),           -- Sales order number
    sls_prd_key NVARCHAR(50),           -- Product key
    sls_cust_id INT,                    -- Customer ID
    sls_order_dt INT,                   -- Order date (INT format assumed)
    sls_ship_dt INT,                    -- Shipping date (INT format assumed)
    sls_due_dt INT,                     -- Due date (INT format assumed)
    sls_sales INT,                      -- Total sales
    sls_quantity INT,                   -- Quantity sold
    sls_price INT                       -- Price per unit
);

-- Drop and recreate ERP tables from source_erp

IF OBJECT_ID('Bronze.erp_loc_101','U') IS NOT NULL DROP TABLE Bronze.erp_loc_101;

CREATE TABLE Bronze.erp_loc_101(
    cid NVARCHAR(50),                   -- Customer ID
    country NVARCHAR(50)                -- Country name
);

IF OBJECT_ID('Bronze.erp_cust_az12','U') IS NOT NULL DROP TABLE Bronze.erp_cust_az12;

CREATE TABLE Bronze.erp_cust_az12(
    cid NVARCHAR(50),                   -- Customer ID
    bdate DATE,                         -- Birthdate
    gen NVARCHAR(50)                    -- Gender
);

IF OBJECT_ID('Bronze.erp_px_cat_g1v2','U') IS NOT NULL DROP TABLE Bronze.erp_px_cat_g1v2;

CREATE TABLE Bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),                    -- Product ID
    cat NVARCHAR(50),                   -- Category
    subcat NVARCHAR(50),                -- Subcategory
    maintanance NVARCHAR(50)            -- Maintenance info
);

-- =============================================
-- Stored Procedure: load_bronze
-- Purpose: Truncate and load Bronze layer tables from CSV files
-- Tracks execution time and prints step-by-step logs
-- =============================================

CREATE OR ALTER PROCEDURE Bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @StartTime DATETIME2,            -- Start time per table
        @EndTime DATETIME2,              -- End time per table
        @TotalStartTime DATETIME2,       -- Start of full process
        @TotalEndTime DATETIME2,         -- End of full process
        @DurationInSeconds FLOAT;        -- Time taken per step or total

    PRINT '=== Starting Bronze Data Load Procedure ===';

    -- Record overall start time
    SET @TotalStartTime = SYSDATETIME();

    BEGIN TRY

        -------------------------------------------------
        -- Step 1: Load CRM Customer Info
        -------------------------------------------------
        PRINT 'Step 1: Truncating Bronze.crm_cust_info...';
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE Bronze.crm_cust_info;
        PRINT 'Step 1: Truncation complete for crm_cust_info.';

        PRINT 'Step 1: Bulk inserting data from cust_info.csv...';
        BULK INSERT Bronze.crm_cust_info
        FROM 'C:\Users\GURU PRAVEEN REDDY J\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 1: Insert complete. Duration: ' + CAST(@DurationInSeconds AS VARCHAR) + ' seconds.';

        -------------------------------------------------
        -- Step 2: Load CRM Product Info
        -------------------------------------------------
        PRINT 'Step 2: Truncating Bronze.crm_prd_info...';
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE Bronze.crm_prd_info;
        PRINT 'Step 2: Truncation complete.';

        PRINT 'Step 2: Bulk inserting from prd_info.csv...';
        BULK INSERT Bronze.crm_prd_info
        FROM 'C:\Users\GURU PRAVEEN REDDY J\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 2: Insert complete. Duration: ' + CAST(@DurationInSeconds AS VARCHAR) + ' seconds.';

        -------------------------------------------------
        -- Step 3: Load CRM Sales Info
        -------------------------------------------------
        PRINT 'Step 3: Truncating Bronze.crm_sales_info...';
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE Bronze.crm_sales_info;
        PRINT 'Step 3: Truncation complete.';

        PRINT 'Step 3: Bulk inserting from sales_details.csv...';
        BULK INSERT Bronze.crm_sales_info
        FROM 'C:\Users\GURU PRAVEEN REDDY J\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 3: Insert complete. Duration: ' + CAST(@DurationInSeconds AS VARCHAR) + ' seconds.';

        -------------------------------------------------
        -- Step 4: Load ERP Customer AZ12
        -------------------------------------------------
        PRINT 'Step 4: Truncating Bronze.erp_cust_az12...';
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE Bronze.erp_cust_az12;
        PRINT 'Step 4: Truncation complete.';

        PRINT 'Step 4: Bulk inserting from CUST_AZ12.csv...';
        BULK INSERT Bronze.erp_cust_az12
        FROM 'C:\Users\GURU PRAVEEN REDDY J\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 4: Insert complete. Duration: ' + CAST(@DurationInSeconds AS VARCHAR) + ' seconds.';

        -------------------------------------------------
        -- Step 5: Load ERP Location Info
        -------------------------------------------------
        PRINT 'Step 5: Truncating Bronze.erp_loc_101...';
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE Bronze.erp_loc_101;
        PRINT 'Step 5: Truncation complete.';

        PRINT 'Step 5: Bulk inserting from LOC_A101.csv...';
        BULK INSERT Bronze.erp_loc_101
        FROM 'C:\Users\GURU PRAVEEN REDDY J\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 5: Insert complete. Duration: ' + CAST(@DurationInSeconds AS VARCHAR) + ' seconds.';

        -------------------------------------------------
        -- Step 6: Load ERP Product Category Info
        -------------------------------------------------
        PRINT 'Step 6: Truncating Bronze.erp_px_cat_g1v2...';
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE Bronze.erp_px_cat_g1v2;
        PRINT 'Step 6: Truncation complete.';

        PRINT 'Step 6: Bulk inserting from PX_CAT_G1V2.csv...';
        BULK INSERT Bronze.erp_px_cat_g1v2
        FROM 'C:\Users\GURU PRAVEEN REDDY J\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 6: Insert complete. Duration: ' + CAST(@DurationInSeconds AS VARCHAR) + ' seconds.';

    END TRY
    BEGIN CATCH
        -- Log errors with severity and number for debugging
        PRINT '❌ An error occurred during data load.';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR);
        RETURN;
    END CATCH

    -- Compute and print total load time
    SET @TotalEndTime = SYSDATETIME();
    SET @DurationInSeconds = DATEDIFF(SECOND, @TotalStartTime, @TotalEndTime);
    PRINT '✅ All steps completed successfully.';
    PRINT '⏱️ Total duration: ' + CAST(@DurationInSeconds AS VARCHAR) + ' seconds.';
END;

-- =============================================
-- Run the procedure to load Bronze layer
-- =============================================
EXEC Bronze.load_bronze;

-- Output current time (optional debug info)
SELECT SYSDATETIME() AS ExecutionFinishedAt;
