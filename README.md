Project Overview:

This project implements an end-to-end ETL pipeline using Apache Airflow, Python (Pandas), and PostgreSQL.

The pipeline processes daily traffic challan data, performs transformations and data quality checks, and loads analytics-ready data into a Postgres database.

Architecture:

CSV File → Extract → Transform → Data Quality Checks → Load → PostgreSQL
The workflow is orchestrated using Airflow DAG scheduling.

----------------------------------------------------------------------------------------------------

Tech Stack:

Python (Pandas)
Apache Airflow
PostgreSQL
Docker (Airflow container setup)
SQLAlchemy

----------------------------------------------------------------------------------------------------

Pipeline Steps:

1️⃣ Extract

Reads raw CSV file (echallan_daily_data.csv)
Saves extracted data as extracted.csv

2️⃣ Transform

Converts date column to datetime

Creates date-based features:
year, month, day
day_of_week
is_weekend

Calculates KPIs:
disposal_rate
pending_rate
avg_amount_per_challan
court_case_ratio

Performs data quality checks:
No null values
No negative totalAmount

3️⃣ Load

Loads transformed data into PostgreSQL table:
traffic_challan_analytics
Uses if_exists="replace" to refresh table

----------------------------------------------------------------------------------------------------

Output:

The final table traffic_challan_analytics contains enriched analytics-ready data suitable for dashboards and reporting.

<img width="952" height="434" alt="image" src="https://github.com/user-attachments/assets/e8c21849-2499-4e2c-b174-f4f858a67ba1" />
