"""Scheduled audit for stalled claims and stream registration/completion gaps."""
import os
import time

import boto3
from boto3.dynamodb.conditions import Attr


TABLE = os.environ.get("LEDGER_TABLE", "commerce-batch-ledger")
NAMESPACE = "CommerceDataPlatform/Operations"


def lambda_handler(_event, _context):
    table = boto3.resource("dynamodb").Table(TABLE)
    now = int(time.time())
    items, start = [], None
    while True:
        args = {"ConsistentRead": True}
        if start:
            args["ExclusiveStartKey"] = start
        page = table.scan(**args)
        items.extend(page.get("Items", []))
        start = page.get("LastEvaluatedKey")
        if not start:
            break
    stuck = sum(1 for item in items if item.get("status") == "PROCESSING" and int(item.get("lease_expires_at", 0)) <= now)
    watermarks = [item for item in items if str(item.get("batch_id", "")).startswith("__watermark__#")]
    gaps = sum(1 for item in watermarks if int(item.get("registered_through_sequence", 0)) != int(item.get("last_completed_sequence", 0)))
    cloudwatch = boto3.client("cloudwatch")
    cloudwatch.put_metric_data(Namespace=NAMESPACE, MetricData=[
        {"MetricName": "StuckProcessing", "Value": stuck, "Unit": "Count"},
        {"MetricName": "WatermarkGaps", "Value": gaps, "Unit": "Count"},
    ])
    return {"stuck_processing": stuck, "watermark_gaps": gaps, "audited_at_epoch": now}
