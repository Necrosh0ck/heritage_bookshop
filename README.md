# Heritage Bookshop - Data Platform Project

A full-stack data engineering initiative integrating APIs, data
pipelines, and business intelligence to support the Heritage Bookshop
ecosystem.

## Project Overview

The **Heritage Bookshop** project delivers an end-to-end data solution
composed of three major components:

1.  **Integration API Development**\
    RESTful APIs built in MuleSoft to standardize and expose sales and
    customer data.

2.  **Data Engineering Pipeline**\
    A Spark-based ingestion and transformation workflow that moves raw
    data into structured PostgreSQL datamarts.

3.  **Business Intelligence**\
    Tableau dashboard enabling insights into sales, customers, and
    operational performance.

## Architecture

    MuleSoft REST APIs
          ↓
    Spark Ingestion Script (batch_ingest.py)
          ↓
    Staging Layer (PostgreSQL)
          ↓
    Datamart Creation Script (datamart_script.py)
          ↓
    Transformation Layer (Spark)
          ↓
    Datamarts (PostgreSQL)
          ↓
    Tableau Dashboard (BI)

## 1. Integration Layer

### Purpose

Provides secure, validated REST endpoints for retrieving and submitting
bookshop-related data.

### Tech Stack

-   MuleSoft Anypoint Studio
-   DataWeave 2.0

### API Types

#### GET Endpoints

Example:

    GET /sales/{transactionId}

Returns: - Transaction ID
- Source system
- Customer information
- Line items
- Payment details

#### POST Endpoints

Example:

    POST /sales

Payload includes: - Transaction data
- Item list
- Customer info
- Metadata

## 2. Data Engineering Pipeline

### Purpose

Automates ingestion, transformation, and preparation of data for
analytics.

### Tech Stack

-   Apache Spark (PySpark)
-   PostgreSQL
-   Python 3.x

### Pipeline Components

#### Batch Ingestion

-   Ingests raw API responses
-   Config-driven ingestion
-   Boilerplate script: `batch_ingest.py`
-   Adds audit fields + PII masking

#### Staging Layer

Stores structured raw data for validation and reconciliation.

#### Transformation Layer

Spark jobs apply: - Cleaning\
- Standardization
- PII masking
- Boilerplate script: `datamart_script.py`
- Business rules

#### Datamart Layer

Optimized for BI consumption.

## 3. Business Intelligence

### Purpose

Enables analytics, reporting, and visualization.

### Tech Stack

-   Tableau Desktop
-   PostgreSQL Datamarts

### Dashboards

-   Sales trends
-   Top-selling items

## Repository Structure

    heritage-bookshop/
    |   ├── business_intelligence
    │   ├── Heritage Bookshop Sales Dashboard.twb
    │   └── heritage_bookshop_sales_dashboard.png
    ├── integration
    │   ├── B987654_response.json
    │   ├── hb_sales_api.png
    │   ├── heritage_bookshop.jar
    │   ├── heritage_bookshop.xml
    │   └── Orange-001_responsse.json
    ├── pipeline
    │   ├── config
    │   │   ├── prod_heritage_bookshop_con.json
    │   │   ├── prod_heritage_bookshop_config.json
    │   │   ├── prod_heritage_bookshop_dm_config.json
    │   │   ├── prod_heritage_bookshop_driver.json
    │   │   └── prod_heritage_bookshop_export_config.json
    │   ├── exported_files
    │   │   ├── dm_sales_customer_demographics_20251124_222024
    │   │   │   ├── _SUCCESS
    │   │   │   └── part-00000-8b338d12-3918-4adf-a01d-d2d40bd5cdd2-c000.csv
    │   │   ├── dm_sales_sales_by_source_20251124_222023
    │   │   │   ├── _SUCCESS
    │   │   │   └── part-00000-d6b5da19-0c71-45d7-bbe8-c58fb6591343-c000.csv
    │   │   ├── dm_sales_sales_full_20251124_222021
    │   │   │   ├── _SUCCESS
    │   │   │   └── part-00000-04e858ff-b9ba-4f4d-ad77-f3e344c49586-c000.csv
    │   │   ├── dm_sales_sales_trend_20251124_222024
    │   │   │   ├── _SUCCESS
    │   │   │   └── part-00000-37df4ea0-8017-4788-b567-d5c0b609c747-c000.csv
    │   │   └── dm_sales_top_selling_products_20251124_222023
    │   │       ├── _SUCCESS
    │   │       └── part-00000-d972c7ac-bb38-4147-a8ee-56f88914d56b-c000.csv
    │   ├── jar
    │   │   └── postgresql-42.7.8.jar
    │   ├── log
    │   │   └── ingest_log.log
    │   └── main
    │       ├── batch_ingest.py
    │       ├── datamart_script.py
    │       └── export_script.py
    └── README.md

## Setup Instructions

### Requirements

-   Python 3.10+
-   Apache Spark
-   PostgreSQL 14+
-   MuleSoft Anypoint Studio
-   Tableau Desktop

### Run Batch Ingestion

    spark-submit \
        --driver-memory 4G \
        --executor-memory 4G \
        --executor-cores 2 \
        --num-executors 2 \
        --jars /Users/josh/Desktop/heritage_bookshop/pipeline/jar/postgresql-42.7.8.jar \
        batch_ingest.py \
        --tables_config /Users/josh/Desktop/heritage_bookshop/pipeline/config/prod_heritage_bookshop_config.json \
        --db_connection /Users/josh/Desktop/heritage_bookshop/pipeline/config/prod_heritage_bookshop_con.json \
        --driver_file /Users/josh/Desktop/heritage_bookshop/pipeline/config/prod_heritage_bookshop_driver.json \
        --log_file /Users/josh/Desktop/heritage_bookshop/pipeline/log/ingest_log.log

## Run Datamart Script

    spark-submit \
    --driver-memory 4G \
    --executor-memory 4G \
    --executor-cores 2 \
    --num-executors 2 \
    --jars /Users/josh/Desktop/heritage_bookshop/pipeline/jar/postgresql-42.7.8.jar \
    datamart_script.py \
    --datamart_config /Users/josh/Desktop/heritage_bookshop/pipeline/config/prod_heritage_bookshop_dm_config.json \
    --db_connection /Users/josh/Desktop/heritage_bookshop/pipeline/config/prod_heritage_bookshop_con.json \
    --log_file /Users/josh/Desktop/heritage_bookshop/pipeline/log/ingest_log.log

## Run Export Script

    spark-submit \
        --driver-memory 4G \
        --executor-memory 4G \
        --executor-cores 2 \
        --num-executors 2 \
        --jars /Users/josh/Desktop/heritage_bookshop/pipeline/jar/postgresql-42.7.8.jar \
        export_script.py \
        --export_tables /Users/josh/Desktop/heritage_bookshop/pipeline/config/prod_heritage_bookshop_export_config.json \
        --db_connection /Users/josh/Desktop/heritage_bookshop/pipeline/config/prod_heritage_bookshop_con.json \
        --log_file /Users/josh/Desktop/heritage_bookshop/pipeline/log/ingest_log.log \
        --output_dir /Users/josh/Desktop/heritage_bookshop/pipeline/exported_files

