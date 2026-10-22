# AWS Commerce Data Platform

A production-style Data Engineering project that simulates an e-commerce company's data platform on AWS.

## Goal

Build an end-to-end data pipeline that:

1. Ingests commerce data from multiple sources.
2. Stores raw data in Amazon S3.
3. Processes data using PySpark and AWS Glue.
4. Applies data-quality validation.
5. Builds analytics-ready models using dbt.
6. Orchestrates workflows with Apache Airflow.
7. Provisions infrastructure with Terraform.
8. Implements automated testing and CI/CD.
9. Monitors pipeline health using AWS CloudWatch.

## Architecture

```text
Sources
   ↓
Python Ingestion
   ↓
S3 Bronze
   ↓
AWS Glue / PySpark
   ↓
Silver
   ↓
dbt
   ↓
Gold
   ↓
Athena / Redshift


## Technical Documentation

The AWS Commerce Data Platform implements an
S3-based commerce data lake with AWS Glue,
PySpark, Apache Iceberg, Amazon Athena,
DynamoDB batch tracking, and AWS monitoring.

### Architecture
[System Architecture](docs/ARCHITECTURE.md)

### Operational Recovery
[Failure Recovery Runbook](docs/RECOVERY_RUNBOOK.md)

### Implemented Capabilities
- Seven commerce datasets in S3 Bronze and Silver
- PySpark transformations and data quality checks
- Rejected-record quarantine
- Glue Data Catalog and Athena SQL analytics
- Apache Iceberg incremental customer MERGE
- SHA-256 batch identity verification
- DynamoDB conditional processing claims
- Heartbeat-based lease renewal
- Idempotent batch replay
- Tested post-commit failure recovery
- CloudWatch operational queries
- EventBridge and SNS failure alert configuration

### Latest Recovery Validation
- Iceberg customers: 1,015
- Completed customer batches: 3
- Recovery attempts for Batch 003: 2
- Additional inserts on recovery: 0
- Additional updates on recovery: 0
- Expired batch claims after recovery: 0

The implementation demonstrates recoverable,
logical idempotency under controlled single-writer
conditions. Automated orchestration and stronger
production fencing are future enhancements.
