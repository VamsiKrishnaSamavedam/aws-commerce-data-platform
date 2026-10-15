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