# Phase 8 Operations Hardening

## Automated publishing

Use `scripts/publish_dataset_manifest.py` to publish a CSV safely. It checks
the dataset contract, calculates the SHA-256 batch identity, obtains the next
registered stream sequence, uploads the source with identity metadata, and
publishes the immutable manifest last. Use `--dry-run` before a new source.

## Recovery drill evidence

Products sequence 3 (`3c9755f64ddf80b3`) was used as an isolated controlled
post-MERGE recovery drill. The first worker attempt failed deliberately during
the test lease cycle. After expiry, the same immutable manifest was replayed
and completed on attempt 2. The Products completion watermark advanced from 2
to 3. This validates recovery for the versioned-upsert path without creating a
new source batch.

## Operator recovery flow

1. Check the dashboard and the batch/watermark records.
2. If the job failed before any Iceberg write, record the failure and replay
   the manifest after correcting the cause.
3. If a MERGE may have committed, do not create a new batch or alter the
   source. Wait for the processing lease to expire.
4. Replay the exact existing `manifest.json`. The registrar now permits an
   expired processing lease to reserve a single reconciliation execution.
5. Confirm Step Functions and Glue succeed, the batch is `COMPLETED`, and
   `last_completed_sequence` advances exactly once.

Immutable append-only datasets require a dedicated reconciliation policy if a
post-MERGE outcome is uncertain, because a direct replay can encounter an
existing immutable key. Treat that as an incident and verify target state
before marking completion.

## Monitoring

The `commerce-data-platform-operations` CloudWatch dashboard shows workflow
failures, registrar errors, DLQ messages, and scheduled audit metrics. The
hourly `commerce-operational-ledger-audit` emits `StuckProcessing` and
`WatermarkGaps`; the stuck-processing alarm notifies the existing pipeline SNS
topic.

## Cost and retention review

No AWS Budget was created, as requested. Existing S3 versioning and the
90-day noncurrent-version retention rule remain in place. Review the dashboard
weekly, remove test Glue workers when no longer needed, and use small test
batches. Before production scale, choose a monthly budget and alert recipient
separately.
