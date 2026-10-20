
from uuid import uuid4

from src.utils.batch_ledger import (
    BatchLedger,
    BatchNotClaimable,
)


def main():
    ledger = BatchLedger()

    dataset = "_ledger_test"
    batch_id = "test-" + uuid4().hex[:16]

    # Retrieve the verified customer batch
    # to reuse its source metadata in this test.

    dynamodb = ledger.table

    response = dynamodb.query(
        KeyConditionExpression=(
            "#pk = :dataset"
        ),
        ExpressionAttributeNames={
            "#pk": "dataset"
        },
        ExpressionAttributeValues={
            ":dataset": "customers"
        },
        ConsistentRead=True,
    )

    completed_records = [
        item
        for item in response["Items"]
        if item.get("status") == "COMPLETED"
        and item.get("registration_mode")
        == "historical_backfill"
    ]

    if not completed_records:
        raise RuntimeError(
            "Historical customer batch not found. "
            "Run backfill first."
        )

    verified_batch = completed_records[0]

    print("Test batch:", batch_id)

    # ------------------------------------------
    # 1. Register PENDING
    # ------------------------------------------

    result = ledger.register_pending(
        dataset=dataset,
        batch_id=batch_id,
        source_s3_uri=verified_batch["source_s3_uri"],
        sha256=verified_batch["sha256"],
    )

    print("1. Registration:", result)

    # ------------------------------------------
    # 2. Claim the batch
    # ------------------------------------------

    token1 = ledger.claim_batch(
        dataset,
        batch_id,
    )

    print("2. First claim: SUCCESS")

    # ------------------------------------------
    # 3. Attempt a second active claim
    # ------------------------------------------

    try:
        ledger.claim_batch(
            dataset,
            batch_id,
        )
        raise AssertionError(
            "Duplicate claim unexpectedly succeeded"
        )

    except BatchNotClaimable:
        print("3. Duplicate claim: BLOCKED")

    # ------------------------------------------
    # 4. Simulate a processing failure
    # ------------------------------------------

    ledger.mark_failed(
        dataset,
        batch_id,
        token1,
        "Intentional test failure; no ETL executed",
    )

    print("4. Mark failed: SUCCESS")

    # ------------------------------------------
    # 5. Retry the same batch
    # ------------------------------------------

    token2 = ledger.claim_batch(
        dataset,
        batch_id,
    )

    assert token2 != token1

    print("5. Retry claim: SUCCESS")

    # ------------------------------------------
    # 6. Complete the retry
    # ------------------------------------------

    ledger.mark_completed(
        dataset,
        batch_id,
        token2,
    )

    print("6. Mark completed: SUCCESS")

    # ------------------------------------------
    # 7. Reject claims after completion
    # ------------------------------------------

    try:
        ledger.claim_batch(
            dataset,
            batch_id,
        )
        raise AssertionError(
            "Completed batch was claimed again"
        )

    except BatchNotClaimable:
        print("7. Completed batch claim: BLOCKED")

    # ------------------------------------------
    # 8. Verify final item
    # ------------------------------------------

    record = ledger.get_batch(
        dataset,
        batch_id,
    )

    assert record["status"] == "COMPLETED"
    assert int(record["attempt_count"]) == 2

    print("Final status:", record["status"])
    print("Attempts:", record["attempt_count"])
    print("All conditional tracking tests PASSED")


if __name__ == "__main__":
    main()
