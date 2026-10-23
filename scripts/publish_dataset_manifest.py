"""Publish an immutable incremental source and manifest for any supported dataset.

The source is uploaded first with identity metadata; the manifest is uploaded
last with If-None-Match so it is an immutable event trigger.
"""
import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3

from src.utils.incremental_datasets import contract


REGION = "us-east-1"
BUCKET = "aws-commerce-data-platform-036071343416-us-east-1"
TABLE = "commerce-batch-ledger"


def next_sequence(table, dataset, stream_id):
    item = table.get_item(
        Key={"dataset": dataset, "batch_id": f"__watermark__#{stream_id}"},
        ConsistentRead=True,
    ).get("Item")
    return int(item.get("registered_through_sequence", 0)) + 1 if item else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--file", required=True, type=Path)
    parser.add_argument("--sequence", type=int)
    parser.add_argument("--stream-id")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    cfg = contract(args.dataset)
    content = args.file.read_bytes()
    rows = list(csv.DictReader(content.decode("utf-8-sig").splitlines()))
    if not rows or tuple(rows[0]) != cfg["columns"]:
        raise ValueError(f"{args.file} does not match the {args.dataset} contract")
    stream_id = args.stream_id or f"{args.dataset}_incremental_v1"
    table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)
    sequence = args.sequence or next_sequence(table, args.dataset, stream_id)
    if sequence < 1:
        raise ValueError("Sequence must be positive")
    digest = hashlib.sha256(content).hexdigest()
    batch_id = digest[:16]
    now = datetime.now(timezone.utc)
    prefix = f"bronze/{args.dataset}/incremental/year={now:%Y}/month={now:%m}/day={now:%d}/batch_id={batch_id}"
    source_key, manifest_key = f"{prefix}/{args.dataset}.csv", f"{prefix}/manifest.json"
    manifest = {
        "schema_version": 1, "dataset": args.dataset, "stream_id": stream_id,
        "sequence": sequence, "batch_id": batch_id,
        "source_s3_uri": f"s3://{BUCKET}/{source_key}", "sha256": digest,
        "row_count": len(rows), "published_at": now.isoformat(),
    }
    print(json.dumps(manifest, indent=2))
    if args.dry_run:
        return
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(Bucket=BUCKET, Key=source_key, Body=content, ContentType="text/csv",
                  Metadata={"sha256": digest, "batch-id": batch_id, "dataset": args.dataset}, IfNoneMatch="*")
    s3.put_object(Bucket=BUCKET, Key=manifest_key, Body=json.dumps(manifest).encode(),
                  ContentType="application/json", IfNoneMatch="*")


if __name__ == "__main__":
    main()
