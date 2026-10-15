# AWS Commerce Data Platform — Learning Notes

## Lesson 1 — Data Engineering Fundamentals

### What is a data pipeline?

A data pipeline moves and processes data from a source to a destination.

### What is a data platform?

A data platform contains pipelines, storage, transformation, orchestration,
testing, monitoring, security, and infrastructure.

### ETL

Extract → Transform → Load

### ELT

Extract → Load → Transform

### Batch processing

Data is collected and processed periodically.

### Streaming processing

Data is processed continuously as events arrive.

### Medallion Architecture

Bronze → Raw data

Silver → Cleaned and validated data

Gold → Business-ready analytical data


## Lesson 2 — Commerce Data Modeling

### Entity
A business object we store data about, such as a customer or order.

### Primary Key
A column that uniquely identifies a record.

Example:
customer_id

### Foreign Key
A column that references the primary key of another table.

Example:
orders.customer_id → customers.customer_id

### One-to-Many
One customer can have many orders.

### Many-to-Many
One order can contain many products and one product can appear in many orders.

We resolve this using the order_items table.

### pandas
A Python library for working with tabular data.

### DataFrame
A two-dimensional tabular data structure in pandas.

### Faker
A Python library used to generate synthetic data.

### created_at
The timestamp when a record was created.

### updated_at
The timestamp when a record was last changed.

updated_at can later support incremental processing.