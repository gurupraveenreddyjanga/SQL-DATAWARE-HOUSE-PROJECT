
# SQL Data Warehouse Project

**🏗️ Data Architecture**

The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
![data_architecture](https://github.com/user-attachments/assets/f66556cf-bf47-408b-a598-b106717c0831)


Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

## 📌 Project Overview
This project involves:

Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
Data Modeling: Developing fact and dimension tables optimized for analytical queries.
Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.
🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:

SQL Development
Data Architect
Data Engineering
ETL Pipeline Developer
Data Modeling
Data Analytics


## 🗂 Data Files

- `crm.csv` – Customer Relationship Management data
- `crp.csv` – Additional business or operational data

All files are located in the `/data/` folder of the repository.

## 🎯 Project Purpose

- Load data from `crm.csv` and `crp.csv` into a SQL database
- Perform ETL processes using SQL scripts
- Clean and standardize the data
- Prepare the data for analysis and reporting

**🚀 Project Requirements****
Building the Data Warehouse (Data Engineering)
Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

Specifications
Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
Data Quality: Cleanse and resolve data quality issues prior to analysis.
Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
Scope: Focus on the latest dataset only; historization of data is not required.
Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

**BI: Analytics & Reporting (Data Analysis)**
Objective
Develop SQL-based analytics to deliver detailed insights into:

Customer Behavior
Product Performance
Sales Trends
These insights empower stakeholders with key business metrics, enabling strategic decision-making.

## 🛠 Technologies Used

- SQL (for database creation, data cleaning, and transformation)
- CSV (raw data format)
- Git & GitHub (for version control)

