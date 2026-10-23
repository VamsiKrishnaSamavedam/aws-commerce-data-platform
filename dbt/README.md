# dbt commerce analytics

This dbt project turns the platform's Iceberg serving tables in
`commerce_incremental` into a separate analytics layer in
`commerce_analytics`.

## Safety boundary

dbt reads only from the existing `*_iceberg` source tables. It writes staging
views and Iceberg marts only to the `commerce_analytics` Glue database and the
configured `s3_data_dir` (`gold_dbt/`). It must never target the ingestion
database or its S3 prefixes.

## Project layout

- `models/sources.yml` maps the seven verified Iceberg source tables.
- `models/staging/` normalizes identifiers, money, quantities, and timestamps.
- `models/marts/` supplies customer/product dimensions and order, order-item,
  payment, shipment, and return facts.
- `macros/` contains the dependency-free data-quality test used by the marts.

## Run locally

Activate `.venv-dbt` from the repository root, then run:

```powershell
dbt debug --project-dir .\dbt --profiles-dir "$HOME\.dbt"
dbt build --project-dir .\dbt --profiles-dir "$HOME\.dbt"
dbt docs generate --project-dir .\dbt --profiles-dir "$HOME\.dbt"
```

Use `dbt build --select staging` to validate typed source views first, or
`dbt build --select marts` to rebuild the analytics marts after a successful
staging run.

## Model grain

- `dim_customers`: one row per customer.
- `dim_products`: one row per product.
- `fct_orders`: one row per order, with item, payment, shipment, and return
  rollups calculated before joins to avoid fan-out.
- `fct_order_items`: one row per immutable order item.
- `fct_payments`: one row per payment attempt.
- `fct_shipments`: one row per shipment.
- `fct_returns`: one row per return.
