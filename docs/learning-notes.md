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

## Lesson 5 — Amazon S3 and Data Ingestion

### Amazon S3

Amazon S3 is AWS object storage.

### Bucket

A bucket is a storage container for S3 objects.

### Object

An object is data stored in S3, such as a CSV or Parquet file.

### Object Key

The object key uniquely identifies an object inside a bucket.

Example:

bronze/customers/year=2026/month=10/day=16/customers.csv

### Prefix

A prefix is part of an S3 object key used to logically organize data.

S3 does not use traditional filesystem folders even though the console
displays prefixes like folders.

### Boto3

Boto3 is the AWS SDK for Python.

Our Python ingestion code uses Boto3 to communicate with Amazon S3.

### Bronze Layer

Bronze contains source data with minimal modification.

### Partitioning

We organize Bronze data using ingestion dates:

year=
month=
day=

### Event Time

The time when a business event occurred.

### Ingestion Time

The time when the data entered our data platform.

### Environment Variable

Configuration stored outside source code.

Example:

COMMERCE_S3_BUCKET

### Idempotency

A pipeline should be designed so that safely rerunning it does not
create incorrect duplicate results.


## Lesson 6 — Production-Style Bronze Ingestion

### Configuration-Driven Pipeline

A generic pipeline can process multiple datasets using configuration
rather than maintaining separate copy-pasted programs.

### DRY

DRY means Don't Repeat Yourself.

Reusable functions reduce duplicated logic and make code easier to
maintain.

### Checksum

A checksum is a value calculated from file contents.

We use SHA-256 to create a fingerprint of each incoming batch.

### SHA-256

SHA-256 is a hashing algorithm.

The same input produces the same hash.

Changing the source data changes the hash.

### Batch ID

Our batch ID is derived from the SHA-256 checksum.

It allows the ingestion pipeline to identify a specific input batch.

### Manifest

A manifest stores metadata describing a successfully ingested batch.

Our manifest contains:

dataset
batch_id
source_file
file_size
checksum
ingestion timestamp
S3 location

### Metadata

Metadata is data that describes other data.

Examples:

file size
checksum
ingestion timestamp
source filename

### Pre-Ingestion Validation

Before uploading, our pipeline verifies that the local source exists,
is a file, and is not empty.

### Post-Upload Validation

After uploading, the pipeline verifies that the S3 object exists and
its size/checksum metadata match expectations.

### Logging

Logging records pipeline activity with timestamps and severity levels.

Logging is more suitable than print statements for production
applications.

### Idempotency

The same successfully processed batch can be safely retried without
creating another copy.

Our implementation uses:

file checksum
→ batch ID
→ S3 manifest
→ skip if already processed


## Lesson 9 — Data Quality and Quarantine

### Data Quality Rule

A condition that valid business data must satisfy.

Examples:

customer_id must exist

first_name must not be blank

email must exist

timestamps must be parseable

updated_at must not occur before created_at


### Quarantine

Quarantine stores records that fail data-quality checks.

Invalid records should not silently disappear from the pipeline.


### Rejection Reason

A rejection reason explains why a record was quarantined.

Examples:

customer_id_missing

email_missing

created_at_invalid

updated_at_before_created_at


### Raw Value Preservation

When parsing source data, the original value can be preserved before
conversion.

Example:

created_at_raw = "not-a-date"

created_at = NULL


### Deterministic Deduplication

Duplicate records should be resolved according to a defined rule.

For customers we keep the record with the latest updated_at timestamp.


### Window Function

A Spark Window allows calculations across related rows without
collapsing them into one aggregated record.

We partition by customer_id and order by updated_at descending.


### row_number

row_number assigns an ordered number within a Window.

The newest customer record receives row number 1.


### Reconciliation

Reconciliation verifies that every input record is accounted for.

input_count =
silver_count +
quarantine_count