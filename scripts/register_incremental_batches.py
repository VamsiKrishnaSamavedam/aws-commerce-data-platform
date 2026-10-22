"""Register validated customer manifests with the DynamoDB sequencing ledger.

The module is both a safe CLI for a known manifest and an AWS Lambda handler
for S3 EventBridge events.  It never starts a Glue job itself.
"""

import argparse
import hashlib
import json
import os
from uuid import uuid4
from urllib.parse import unquote_plus, urlparse

import boto3
from boto3.dynamodb.conditions import Attr

from src.utils.batch_ledger import BatchLedger
from src.utils.incremental_manifest import (
    ManifestValidationError,
    validate_dataset_manifest,
)


AWS_REGION = "us-east-1"
BUCKET = "aws-commerce-data-platform-036071343416-us-east-1"
MAX_SOURCE_BYTES = 10 * 1024 * 1024
GLUE_JOBS = {
    "customers": "commerce-customers-iceberg-merge-ledger",
    "products": "commerce-products-incremental-iceberg",
    "orders": "commerce-orders-incremental-iceberg",
    "order_items": "commerce-order-items-incremental-iceberg",
    "payments": "commerce-payments-incremental-iceberg",
    "shipments": "commerce-shipments-incremental-iceberg",
    "returns": "commerce-returns-incremental-iceberg",
}


def read_and_verify_manifest(manifest_uri, s3_client=None):
    """Read one manifest and verify the referenced customer object identity."""
    parsed = urlparse(manifest_uri)
    if parsed.scheme != "s3" or parsed.netloc != BUCKET:
        raise ManifestValidationError("Unexpected manifest bucket")
    if not parsed.path.endswith("/manifest.json"):
        raise ManifestValidationError("Event does not reference a manifest")

    s3_client = s3_client or boto3.client("s3", region_name=AWS_REGION)
    raw_manifest = s3_client.get_object(
        Bucket=BUCKET,
        Key=parsed.path.lstrip("/"),
    )["Body"].read()
    if len(raw_manifest) > MAX_SOURCE_BYTES:
        raise ManifestValidationError("Manifest exceeds the safety limit")

    try:
        manifest = json.loads(raw_manifest)
    except json.JSONDecodeError as error:
        raise ManifestValidationError("Manifest is not valid JSON") from error

    normalized = validate_dataset_manifest(manifest, BUCKET)
    head = s3_client.head_object(Bucket=BUCKET, Key=normalized["source_key"])
    metadata = head.get("Metadata", {})
    if head["ContentLength"] < 1 or head["ContentLength"] > MAX_SOURCE_BYTES:
        raise ManifestValidationError("Source object violates size safety limit")
    if (
        metadata.get("sha256") != normalized["sha256"]
        or metadata.get("batch-id") != normalized["batch_id"]
        or metadata.get("dataset") != normalized["dataset"]
    ):
        raise ManifestValidationError("Source metadata differs from manifest")

    content = s3_client.get_object(
        Bucket=BUCKET,
        Key=normalized["source_key"],
    )["Body"].read()
    if hashlib.sha256(content).hexdigest() != normalized["sha256"]:
        raise ManifestValidationError("Source content checksum differs from manifest")
    return normalized


def register_manifest(manifest_uri, s3_client=None, ledger=None):
    """Register one manifest and return an orchestration-safe action."""
    manifest = read_and_verify_manifest(manifest_uri, s3_client)
    ledger = ledger or BatchLedger()
    result = ledger.register_sequenced_pending(manifest)
    batch = ledger.get_batch(manifest["dataset"], manifest["batch_id"])
    watermark = ledger.get_watermark(
        manifest["dataset"], manifest["stream_id"],
    )
    completed = int(watermark.get("last_completed_sequence", 0))

    if batch["status"] == "COMPLETED":
        action = "SKIP_COMPLETED"
    elif batch["status"] == "PROCESSING":
        action = "SKIP_ACTIVE"
    elif manifest["sequence"] == completed + 1:
        if ledger.reserve_orchestration(
            manifest["dataset"], manifest["batch_id"],
        ):
            action = "START_GLUE"
        else:
            action = "SKIP_ALREADY_DISPATCHED"
    else:
        action = "WAIT_FOR_PREDECESSOR"

    return {
        "action": action,
        "registration": result,
        "dataset": manifest["dataset"],
        "batch_id": manifest["batch_id"],
        "sequence": manifest["sequence"],
        "source_s3_uri": manifest["source_s3_uri"],
        "glue_job_name": GLUE_JOBS[manifest["dataset"]],
        "last_completed_sequence": completed,
    }


def event_manifest_uri(event):
    """Extract a manifest URI from EventBridge S3 or direct invocation input."""
    if event.get("manifest_uri"):
        return event["manifest_uri"]
    detail = event.get("detail", {})
    bucket = detail.get("bucket", {}).get("name")
    key = detail.get("object", {}).get("key")
    if bucket and key:
        return f"s3://{bucket}/{unquote_plus(key)}"
    raise ManifestValidationError("No manifest URI found in event")


def lambda_handler(event, _context):
    """Lambda entry point. Exceptions are deliberate so EventBridge can retry."""
    if event.get("operation") == "dispatch_next":
        return dispatch_next(event)
    if event.get("operation") == "verify_completion":
        return verify_completion(event)
    return register_manifest(event_manifest_uri(event))


def verify_completion(event, ledger=None):
    """Confirm Glue acknowledged both the batch and its durable watermark."""
    ledger = ledger or BatchLedger()
    dataset = event["dataset"]
    batch = ledger.get_batch(dataset, event["batch_id"])
    if batch is None:
        raise RuntimeError("Batch disappeared before completion verification")
    watermark = ledger.get_watermark(dataset, batch["stream_id"])
    complete = (
        batch.get("status") == "COMPLETED"
        and watermark is not None
        and int(watermark.get("last_completed_sequence", 0))
        >= int(batch["sequence"])
    )
    return {
        "action": "COMPLETED" if complete else "INCOMPLETE",
        "dataset": dataset,
        "batch_id": batch["batch_id"],
        "sequence": int(batch["sequence"]),
        "stream_id": batch["stream_id"],
    }


def dispatch_next(event, ledger=None, states_client=None):
    """Reserve and start the next ready sequence after a successful batch."""
    dataset = event.get("dataset", "customers")
    stream_id = event.get("stream_id", "customers_incremental_v1")
    ledger = ledger or BatchLedger()
    watermark = ledger.get_watermark(dataset, stream_id)
    if watermark is None:
        return {"action": "NO_WATERMARK"}
    expected = int(watermark.get("last_completed_sequence", 0)) + 1

    candidates = []
    eligible = (
        Attr("dataset").eq(dataset)
        & Attr("status").is_in(["PENDING", "FAILED"])
    )
    scan = ledger.table.scan(FilterExpression=eligible, ConsistentRead=True)
    candidates.extend(scan.get("Items", []))
    while "LastEvaluatedKey" in scan:
        scan = ledger.table.scan(
            FilterExpression=eligible,
            ExclusiveStartKey=scan["LastEvaluatedKey"],
            ConsistentRead=True,
        )
        candidates.extend(scan.get("Items", []))

    ready = [
        item for item in candidates
        if item.get("stream_id") == stream_id
        and int(item.get("sequence", -1)) == expected
    ]
    if len(ready) != 1:
        return {"action": "NO_READY_BATCH", "expected_sequence": expected}

    batch = ready[0]
    if not ledger.reserve_orchestration(dataset, batch["batch_id"]):
        return {"action": "SKIP_ALREADY_DISPATCHED"}

    manifest_uri = batch["source_s3_uri"].rsplit("/", 1)[0] + "/manifest.json"
    state_machine_arn = os.environ["STATE_MACHINE_ARN"]
    states_client = states_client or boto3.client(
        "stepfunctions", region_name=AWS_REGION,
    )
    states_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=f"{batch['batch_id']}-{uuid4().hex[:8]}",
        input=json.dumps({"manifest_uri": manifest_uri}),
    )
    return {
        "action": "STARTED_NEXT",
        "batch_id": batch["batch_id"],
        "sequence": expected,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-uri", required=True)
    args = parser.parse_args()
    print(json.dumps(register_manifest(args.manifest_uri), indent=2))


if __name__ == "__main__":
    main()
