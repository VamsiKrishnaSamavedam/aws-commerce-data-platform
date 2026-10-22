# Phase 7 Incremental Commerce Datasets

## Delivered datasets

| Dataset | Write behavior | Parent checks |
| --- | --- | --- |
| Products | Versioned upsert on `product_id` | None |
| Orders | Versioned upsert on `order_id` | Customers |
| Order items | Immutable append-only on `order_item_id` | Orders, Products |
| Payments | Immutable append-only on `payment_id` | Orders |
| Shipments | Versioned upsert on `shipment_id` | Orders |
| Returns | Immutable append-only on `return_id` | Orders, Products |

Each dataset has an Iceberg bootstrap job and shares the manifest, DynamoDB
ledger, processing lease, and sequenced completion-watermark controls.

## Publication contract

Upload the CSV with SHA-256, `batch-id`, and `dataset` object metadata. Publish
`manifest.json` last beneath:

`bronze/<dataset>/incremental/year=YYYY/month=MM/day=DD/batch_id=<sha-prefix>/<dataset>.csv`

The manifest sequence starts at 1 for each dataset stream. EventBridge invokes
the state machine, which validates the manifest, selects the allow-listed Glue
job, and verifies the ledger completion result.

## Verified evidence

On 2026-10-22, all six baseline Iceberg tables bootstrapped successfully. The
Products stream then processed two one-row immutable verification manifests:

- Sequence 1 (`aa83bcf70fb97fc2`) completed and advanced its watermark to 1.
- Sequence 2 (`c95b3cf5e64913a8`) completed through EventBridge, Step
  Functions, Glue, and DynamoDB; its completion watermark is 2.

An initial sequence-1 execution exposed a missing `dataset` field in the
verifier response. Glue and the ledger had completed correctly, but the final
state-machine result mapping failed. The response was fixed and deployed
before sequence 2; sequence 2's state-machine execution succeeded.

## Operating checks

For every new manifest, confirm all four conditions:

1. The Step Functions execution is `SUCCEEDED`.
2. The selected Glue job run is `SUCCEEDED`.
3. The DynamoDB batch is `COMPLETED`.
4. `last_completed_sequence` equals the batch sequence for its stream.

If Glue may have written before failing, do not manually mark the batch failed.
Allow the lease to expire, investigate, and replay the same immutable manifest.
