# Phase 6 Operations

## Verified flow

1. A customer CSV is uploaded below the incremental Bronze prefix.
2. An immutable `manifest.json` is published last with a stream sequence,
   content hash, row count, and source URI.
3. S3 sends an Object Created event to EventBridge. The rule starts the
   `commerce-incremental-customer-orchestrator` state machine.
4. The registrar validates the manifest and source checksum, atomically
   registers the batch, and advances the registration watermark.
5. Only the next completion sequence can reserve orchestration and claim the
   ledger processing lease. Glue performs the Iceberg MERGE and reconciliation.
6. Glue atomically marks the batch complete and advances the completion
   watermark. The workflow verifies both records before succeeding.

## Operational checks

- Step Functions: inspect the execution status and failure cause.
- DynamoDB: read the batch record and
  `__watermark__#customers_incremental_v1` with consistent reads.
- Glue: inspect the corresponding job run and CloudWatch logs.
- EventBridge: inspect the rule target and the encrypted DLQ for undelivered
  manifest events.

## Recovery

Do not manually mark a post-MERGE failure as `FAILED`. The existing Glue
replay and Iceberg reconciliation logic handles uncertain commit outcomes.
Investigate the job run, wait for the processing lease to expire when needed,
then replay the same immutable manifest. Duplicate delivery is safe: the
registration transaction and orchestration lease prevent a second active run.

## Validated batches

- Sequence 1: `82edcad1ef55bde5` completed through the new control plane.
- Sequence 2: EventBridge-triggered validation batch; completion evidence is
  recorded in the ledger and workflow history.
