# scalable-data-engineering-pipeline

## Overview

This project is an end-to-end data engineering pipeline implementing a scalable Bronze-Silver-Gold architecture. It focuses on reliable data ingestion, data quality, and business-ready transformations rather than data visualization or dashboards. The pipeline is designed to transform raw CSV data into clean, structured, and analytics-ready datasets that can be consumed by BI tools or downstream analytics systems.

---

## Dataset Source

The project uses the **Brazilian E-Commerce Public Dataset by Olist**, available on Kaggle at the following link:  
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

## Project Goals

The main objectives of this project are:

- Build a production-style data pipeline
- Apply data lake best practices using layered architecture
- Ensure data quality and schema consistency
- Separate raw data handling from business logic
- Prepare analytics-ready datasets for customer and revenue analysis

---

## Architecture Overview

The pipeline follows a standard layered data lake design:

**Source:** CSV files are ingested into

**Bronze layer:** Raw data stored in Parquet format with ingestion metadata

**Silver layer:** Cleaned, typed, and deduplicated datasets with consistent schemas

**Gold layer:** Business-oriented tables and KPIs ready for analytics and BI tools

---

## Project Structure

The repository is organized as follows:

```
data_lake/
  ├── raw/          # Bronze layer data
  ├── staging/      # Silver layer data
  └── curated/      # Gold layer data

source_data/        # Original CSV files from the dataset

ingestion/          # Scripts responsible for Bronze ingestion

transformations/
  ├── staging/      # Silver transformations
  └── curated/      # Gold business transformations

venv/               # Local Python virtual environment

notebooks/          # Optional exploration notebooks
```

---

## Technology Stack

- **Programming language:** Python 3
- **Libraries:** pandas, pyarrow
- **Storage format:** Parquet
- **Environment:** Virtualenv (without Anaconda)
- **Operating system:** Developed and tested on Windows using command prompt

---

## Bronze Layer Description

The Bronze layer is responsible for ingesting raw CSV files without applying any business logic. Each ingestion adds metadata columns such as ingestion timestamp, ingestion date, source file, and data source. Data is stored in Parquet format and partitioned by ingestion date to support scalability and reprocessing.

---

## Silver Layer Description

The Silver layer cleans and standardizes the data. Typical transformations include:

- Type casting for dates and numeric columns
- Deduplication using business keys
- Handling missing and invalid values
- Schema consistency across ingestions

The output of this layer represents trusted datasets ready for business transformations.

---

## Gold Layer Description

The Gold layer contains business-ready datasets optimized for analytics. Transformations include aggregations, KPIs, and segmentation such as:

- Orders per day and per status
- Revenue and average order value
- Delivery performance metrics
- Customer geographic segmentation

Gold tables are flat, aggregated, and designed to be easily consumed by BI tools. They are exported in both Parquet and CSV formats.

---

## BI and Analytics Usage

This project intentionally does not focus on dashboards or visualization design. Instead, it prepares high-quality datasets that can be directly consumed by tools such as Power BI, Tableau, Excel, or Python visualization libraries.

The BI layer is fully decoupled from the data engineering logic.

---

## Why This Project Matters

This repository demonstrates real data engineering skills including:

- Pipeline design
- Layered architecture
- Data quality handling
- Business-oriented transformations
- Scalable and reproducible workflows

It is not a notebook-based demo, but a structured data engineering project.

---

## Possible Extensions

- Workflow orchestration with Airflow or Prefect
- Migration to Spark or PySpark
- Incremental ingestion
- Data quality tests
- Cloud storage integration
- CI/CD for data pipelines

---

## License

This project is licensed under the MIT License.