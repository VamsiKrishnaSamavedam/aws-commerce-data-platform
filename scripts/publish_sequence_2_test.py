"""Publish one small, immutable sequence-2 test batch for event-path validation."""

import csv
import hashlib
import io
import json
from datetime import datetime, timezone

import boto3


BUCKET = "aws-commerce-data-platform-036071343416-us-east-1"


def main():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for number in range(1, 6):
        rows.append({
            "customer_id": f"CUST_{number:04d}",
            "first_name": f"Updated{number}", "last_name": "Customer",
            "email": f"customer{number}@example.com", "country": "Canada",
            "created_at": "2026-01-01 00:00:00", "updated_at": timestamp,
        })
    for number in range(21, 26):
        rows.append({
            "customer_id": f"INC_CUST_{number:04d}",
            "first_name": f"Sequence{number}", "last_name": "Test",
            "email": f"sequence{number}@example.com", "country": "Canada",
            "created_at": timestamp, "updated_at": timestamp,
        })
    columns = list(rows[0])
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    body = buffer.getvalue().encode()
    digest = hashlib.sha256(body).hexdigest()
    batch_id = digest[:16]
    prefix = (
        f"bronze/customers/incremental/year={now:%Y}/month={now:%m}/"
        f"day={now:%d}/batch_id={batch_id}"
    )
    source_key = f"{prefix}/customers.csv"
    manifest_key = f"{prefix}/manifest.json"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket=BUCKET, Key=source_key, Body=body,
                  Metadata={"dataset": "customers", "batch-id": batch_id, "sha256": digest})
    manifest = {"schema_version": 1, "dataset": "customers",
                "stream_id": "customers_incremental_v1", "sequence": 2,
                "batch_id": batch_id, "source_s3_uri": f"s3://{BUCKET}/{source_key}",
                "sha256": digest, "row_count": len(rows), "published_at": now.isoformat()}
    s3.put_object(Bucket=BUCKET, Key=manifest_key,
                  Body=json.dumps(manifest).encode(), ContentType="application/json",
                  IfNoneMatch="*")
    print(json.dumps({"batch_id": batch_id, "manifest_uri": f"s3://{BUCKET}/{manifest_key}"}))


if __name__ == "__main__":
    main()
