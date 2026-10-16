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


## Lesson 3 — Relational Synthetic Data

### Reproducibility

Using a fixed random seed allows synthetic datasets to be generated
predictably across multiple runs.

Examples:

random.seed(42)

Faker.seed(42)


### Referential Integrity

Foreign-key values must reference existing parent records.

Example:

orders.customer_id must exist in customers.customer_id


### Orphan Record

A child record whose foreign key references a parent record that
does not exist.


### Data Invariant

A condition that should always remain true.

Examples:

customer_id must be unique

created_at <= updated_at

price > 0

quantity > 0


### Weighted Randomness

Synthetic data distributions can use probabilities instead of making
every possible value equally likely.


### Order Items

order_items resolves the many-to-many relationship between orders
and products.


### Derived Data

orders.total_amount should be calculated from order_items instead
of independently generated.

Formula:

total_amount =
SUM(quantity * unit_price)


### assert

Python assert statements can be used during development to verify
assumptions and stop execution when an expected condition is false.


## Lesson 4 — Cross-Table Business Rules

### Business Rule

A condition that describes how valid business data should behave.

Examples:

- Cancelled orders should not be shipped.
- Payment amount should match order total.
- Shipment date cannot occur before order date.
- Return date cannot occur before delivery date.


### Referential Integrity

Child records must reference legitimate parent records.


### Composite Relationship Validation

Sometimes validating each foreign key independently is not enough.

Example:

A returned product must not only exist, and the order must not only
exist — the product must actually belong to that exact order.


### Data Lineage

Data lineage describes where data comes from and how it moves or
relates across systems and transformations.


### pandas merge

A pandas merge combines DataFrames using matching columns and is
conceptually similar to a SQL JOIN.


### continue

The continue statement skips the remaining code in the current loop
iteration and moves to the next iteration.


### Lookup Structure

A dictionary can provide fast access to related values using a key,
such as looking up a delivery date using an order_id.