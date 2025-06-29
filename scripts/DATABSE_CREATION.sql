/*This repository contains SQL scripts for initializing a Data Warehouse database in Microsoft SQL Server. The script performs the following actions:

Drops the existing DataWareHouse database (if it exists).

Creates a fresh DataWareHouse database.

Sets up a typical multi-layered schema architecture:

Bronze – Raw or ingested data.

Silver – Cleaned and transformed data.

Gold – Curated data ready for analytics and reporting.

*/

USE master;
GO

-- Drop the DataWareHouse database if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')
BEGIN 
    ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWareHouse;
END;
GO

-- Create the DataWareHouse database
CREATE DATABASE DataWareHouse;
GO

-- Switch to the new database
USE DataWareHouse;
GO

-- Create schemas
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO
