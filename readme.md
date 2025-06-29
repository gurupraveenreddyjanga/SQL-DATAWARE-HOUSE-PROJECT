
# SQL Data Warehouse Project

## 📌 Project Overview
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers: Data Architecture

Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.
This **SQL Data Warehouse Project** is focused on loading and transforming data from CSV files into a SQL database. The main purpose is to perform ETL (Extract, Transform, Load), clean the data, and enable data analytics for business insights.

## 🗂 Data Files

- `crm.csv` – Customer Relationship Management data
- `crp.csv` – Additional business or operational data

All files are located in the `/data/` folder of the repository.

## 🎯 Project Purpose

- Load data from `crm.csv` and `crp.csv` into a SQL database
- Perform ETL processes using SQL scripts
- Clean and standardize the data
- Prepare the data for analysis and reporting

## 🛠 Technologies Used

- SQL (for database creation, data cleaning, and transformation)
- CSV (raw data format)
- Git & GitHub (for version control)

