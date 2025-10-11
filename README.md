# ShopStream Analytics

## Overview
**ShopStream Analytics** is a modern data engineering project that demonstrates how to build a real-time Data Warehouse (DWH) and analytics platform using **streaming, batch processing, and medallion architecture**.  
The project ingests e-commerce datasets (customers, orders, products, events) from the **DummyJSON API** and processes them through an end-to-end pipeline.

---

## Pipeline Flow
![Pipeline Architecture](docs/PipelineArchitectur.jpg) 
1. **Ingestion**: Kafka producers fetch data from DummyJSON API and publish events.  
2. **Bronze Layer**: Kafka consumers write raw JSON into PostgreSQL.  
3. **Silver Layer**: Spark jobs (structured streaming + batch) clean and normalize data.  
4. **Gold Layer**: dbt models create analytics-ready tables (facts & dimensions).  
5. **Visualization**: BI dashboards show insights (sales, customers, product trends, events).  


## Architecture
The project follows the **Medallion Architecture**:

- **Bronze Layer (Raw Data)**  
  - Ingest raw data from DummyJSON API.  
  - Data stored in **PostgreSQL** (raw schema).  
  - Kafka streams simulate continuous ingestion.

- **Silver Layer (Cleaned & Processed Data)**  
  - Transformations using **DBT** (streaming + batch).  
  - Data deduplication, cleaning, and normalization.  
  - Stored in PostgreSQL (silver schema).

- **Gold Layer (Analytics-Ready Data)**  
  - Business logic and transformations via **dbt**.  
  - Fact and Dimension tables for analytics.  
  - Stored in PostgreSQL (gold schema).

## Data Model

```mermaid
erDiagram
    %% External Data Sources
    DummyJSON_API {
        string users_endpoint "/users"
        string products_endpoint "/products"
        string carts_endpoint "/carts"
        string posts_endpoint "/posts"
    }

    %% Kafka Topics
    Kafka_Topics {
        string customers_topic "customers"
        string products_topic "products"
        string orders_topic "orders"
        string events_topic "events"
    }

    %% Bronze Layer - Raw Data
    bronze_raw_events {
        string topic "Kafka topic name"
        string record_key "Kafka message key"
        jsonb record_value "JSON payload"
        int partition "Kafka partition"
        bigint offset "Kafka offset"
        timestamp event_ts "Event timestamp"
        timestamp ingested_at "Ingestion timestamp"
    }

    %% Silver Layer - Cleaned Data
    silver_customers {
        string customer_id PK "Primary key"
        string first_name "Customer first name"
        string last_name "Customer last name"
        string email "Customer email"
        timestamp created_at "Record creation time"
        timestamp updated_at "Last update time"
    }

    silver_products {
        string product_id PK "Primary key"
        string name "Product name"
        text description "Product description"
        decimal price "Product price"
        string category "Product category"
        timestamp created_at "Record creation time"
        timestamp updated_at "Last update time"
    }

    silver_orders {
        string order_id PK "Primary key"
        string customer_id FK "Foreign key to customers"
        timestamp order_date "Order date"
        decimal total_amount "Total order amount"
        string status "Order status"
        timestamp created_at "Record creation time"
        timestamp updated_at "Last update time"
    }

    silver_order_items {
        string order_item_id PK "Primary key"
        string order_id FK "Foreign key to orders"
        string product_id FK "Foreign key to products"
        int quantity "Quantity ordered"
        decimal unit_price "Price per unit"
        timestamp created_at "Record creation time"
    }

    silver_events {
        string event_id PK "Primary key"
        string customer_id FK "Foreign key to customers"
        string event_type "Type of event"
        jsonb event_data "Event payload data"
        timestamp created_at "Event timestamp"
    }

    %% Gold Layer - Analytics Ready
    dim_customers {
        string customer_id PK "Primary key"
        string first_name "Customer first name"
        string last_name "Customer last name"
        string email "Customer email"
        timestamp created_at "Record creation time"
        timestamp updated_at "Last update time"
    }

    dim_products {
        string product_id PK "Primary key"
        string name "Product name"
        text description "Product description"
        decimal price "Product price"
        string category "Product category"
        timestamp created_at "Record creation time"
        timestamp updated_at "Last update time"
    }

    fct_orders {
        string order_id PK "Primary key"
        string customer_id FK "Foreign key to dim_customers"
        timestamp order_date "Order date"
        decimal total_amount "Total order amount"
        string status "Order status"
        string customer_email "Customer email"
    }

    fct_order_items {
        string order_item_id PK "Primary key"
        string order_id FK "Foreign key to fct_orders"
        string product_id FK "Foreign key to dim_products"
        int quantity "Quantity ordered"
        decimal unit_price "Price per unit"
        decimal line_total "Quantity * unit_price"
        timestamp order_date "Order date"
        string customer_id "Customer ID"
    }

    agg_orders_daily {
        date order_date PK "Order date"
        int orders "Number of orders"
        decimal revenue "Total revenue"
    }

    agg_events_daily {
        date event_date PK "Event date"
        string event_type PK "Event type"
        int events "Number of events"
    }

    %% Relationships
    DummyJSON_API ||--o{ Kafka_Topics : "API calls"
    Kafka_Topics ||--o{ bronze_raw_events : "Kafka messages"
    
    bronze_raw_events ||--o{ silver_customers : "topic='customers'"
    bronze_raw_events ||--o{ silver_products : "topic='products'"
    bronze_raw_events ||--o{ silver_orders : "topic='orders'"
    bronze_raw_events ||--o{ silver_order_items : "topic='orders'"
    bronze_raw_events ||--o{ silver_events : "topic='events'"

    silver_customers ||--o{ silver_orders : "customer_id"
    silver_orders ||--o{ silver_order_items : "order_id"
    silver_products ||--o{ silver_order_items : "product_id"
    silver_customers ||--o{ silver_events : "customer_id"

    silver_customers ||--o{ dim_customers : "dbt transformation"
    silver_products ||--o{ dim_products : "dbt transformation"
    silver_orders ||--o{ fct_orders : "dbt transformation"
    silver_order_items ||--o{ fct_order_items : "dbt transformation"
    silver_orders ||--o{ agg_orders_daily : "dbt aggregation"
    silver_events ||--o{ agg_events_daily : "dbt aggregation"

    dim_customers ||--o{ fct_orders : "customer_id"
    dim_products ||--o{ fct_order_items : "product_id"
    fct_orders ||--o{ fct_order_items : "order_id"
```

---

<!-- ## Bronze Layer — Raw Data

Stores data exactly as it comes from DummyJSON API via Kafka producers. Raw events are appended to Postgres in `bronze.raw_events`.

- **Kafka → Postgres loader**: `kafka/consumers/postgres_loader.py`
  - Reads topics from `KAFKA_TOPICS` (default: `customers,orders,products`).
  - Ensures `bronze` schema and `bronze.raw_events` table exist.
  - Writes each message with metadata: `topic`, `record_key`, `record_value` (JSONB), `partition`, `offset`, `event_ts`, `ingested_at`.

- **Spark Structured Streaming (optional)**: `kafka/consumers/spark_streaming.py`
  - Reads the same Kafka topics and writes to `bronze.raw_events` via JDBC using `foreachBatch`. -->



<!-- ## Tech Stack
- **PostgreSQL** → Data Warehouse  
- **Apache Spark** → Real-time & batch processing  
- **Apache Kafka** → Streaming & message ingestion  
- **dbt** → Data transformations & modeling  
- **Airflow** → Orchestration & scheduling  
- **DummyJSON API** → Real-world e-commerce data source  
- **Power BI** → Visualization & dashboards   -->



<!-- ## Data Sources
The project uses the following API endpoints from DummyJSON:  
- `customers.raw` → `/users`  
- `orders.raw` → `/carts`  
- `products.raw` → `/products`  
- `events.raw` → `/posts`  -->



