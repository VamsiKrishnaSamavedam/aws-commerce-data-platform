
from datetime import datetime, timezone
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError


TABLE_NAME = "commerce-batch-ledger"
AWS_REGION = "us-east-1"


class BatchConflict(RuntimeError):
    pass


class BatchNotClaimable(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat()


def is_condition_failure(error):
    return (
        error.response["Error"]["Code"]
        == "ConditionalCheckFailedException"
    )


class BatchLedger:

    def __init__(self):
        dynamodb = boto3.resource(
            "dynamodb",
            region_name=AWS_REGION,
        )

        self.table = dynamodb.Table(TABLE_NAME)

    # ----------------------------------------------
    # Retrieve a batch
    # ----------------------------------------------

    def get_batch(self, dataset, batch_id):
        result = self.table.get_item(
            Key={
                "dataset": dataset,
                "batch_id": batch_id,
            },
            ConsistentRead=True,
        )

        return result.get("Item")

    # ----------------------------------------------
    # Insert a record only if it does not exist
    # ----------------------------------------------

    def _insert_once(self, item):
        try:
            self.table.put_item(
                Item=item,
                ConditionExpression=(
                    "attribute_not_exists(#pk)"
                ),
                ExpressionAttributeNames={
                    "#pk": "dataset"
                },
            )
            return True

        except ClientError as error:
            if is_condition_failure(error):
                return False
            raise

    # ----------------------------------------------
    # Register a new unprocessed batch
    # ----------------------------------------------

    def register_pending(
        self,
        dataset,
        batch_id,
        source_s3_uri,
        sha256,
    ):
        now = iso_now()

        item = {
            "dataset": dataset,
            "batch_id": batch_id,
            "source_s3_uri": source_s3_uri,
            "sha256": sha256,
            "status": "PENDING",
            "attempt_count": 0,
            "created_at": now,
            "updated_at": now,
        }

        inserted = self._insert_once(item)

        if inserted:
            return "REGISTERED"

        existing = self.get_batch(dataset, batch_id)

        if existing is None:
            raise RuntimeError(
                "Registration conflict; retry lookup."
            )

        if (
            existing.get("source_s3_uri")
            != source_s3_uri
            or existing.get("sha256") != sha256
        ):
            raise BatchConflict(
                "Batch ID already exists with "
                "different source metadata."
            )

        return "ALREADY_REGISTERED"

    # ----------------------------------------------
    # Backfill an already completed historical batch
    # ----------------------------------------------

    def backfill_completed(
        self,
        dataset,
        batch_id,
        source_s3_uri,
        sha256,
    ):
        now = iso_now()

        item = {
            "dataset": dataset,
            "batch_id": batch_id,
            "source_s3_uri": source_s3_uri,
            "sha256": sha256,
            "status": "COMPLETED",
            "attempt_count": 0,
            "created_at": now,
            "updated_at": now,
            "completion_recorded_at": now,
            "registration_mode": "historical_backfill",
            "verification_method": (
                "previous_iceberg_merge_and_"
                "athena_replay_verified"
            ),
            "historical_attempts_untracked": True,
        }

        inserted = self._insert_once(item)

        if inserted:
            return "BACKFILLED_COMPLETED"

        existing = self.get_batch(dataset, batch_id)

        if existing is None:
            raise RuntimeError(
                "Backfill conflict; retry lookup."
            )

        if (
            existing.get("source_s3_uri")
            != source_s3_uri
            or existing.get("sha256") != sha256
        ):
            raise BatchConflict(
                "Existing batch has different "
                "source metadata."
            )

        if existing.get("status") == "COMPLETED":
            return "ALREADY_COMPLETED"

        raise BatchConflict(
            "Existing batch is not COMPLETED. "
            "Do not overwrite its current status."
        )

    # ----------------------------------------------
    # Claim a batch for processing
    # ----------------------------------------------

    def claim_batch(
        self,
        dataset,
        batch_id,
        lease_seconds=1800,
    ):
        now = utc_now()
        now_epoch = int(now.timestamp())
        lease_until = now_epoch + lease_seconds

        token = str(uuid4())

        try:
            self.table.update_item(
                Key={
                    "dataset": dataset,
                    "batch_id": batch_id,
                },
                UpdateExpression=(
                    "SET #st = :processing, "
                    "processing_token = :token, "
                    "lease_expires_at = :lease, "
                    "started_at = :now_text, "
                    "updated_at = :now_text, "
                    "attempt_count = "
                    "if_not_exists(attempt_count, :zero) "
                    "+ :one "
                    "REMOVE last_error"
                ),
                ConditionExpression=(
                    "#st IN (:pending, :failed) "
                    "OR "
                    "(#st = :processing AND "
                    "lease_expires_at < :epoch)"
                ),
                ExpressionAttributeNames={
                    "#st": "status",
                },
                ExpressionAttributeValues={
                    ":pending": "PENDING",
                    ":failed": "FAILED",
                    ":processing": "PROCESSING",
                    ":token": token,
                    ":lease": lease_until,
                    ":epoch": now_epoch,
                    ":now_text": now.isoformat(),
                    ":zero": 0,
                    ":one": 1,
                },
            )

        except ClientError as error:
            if is_condition_failure(error):
                raise BatchNotClaimable(
                    "Batch already completed, "
                    "actively claimed, or missing."
                ) from error
            raise

        return token

    # ----------------------------------------------
    # Mark the current owner's batch as completed
    # ----------------------------------------------

    def mark_completed(
        self,
        dataset,
        batch_id,
        token,
    ):
        now = utc_now()

        try:
            self.table.update_item(
                Key={
                    "dataset": dataset,
                    "batch_id": batch_id,
                },
                UpdateExpression=(
                    "SET #st = :completed, "
                    "completed_at = :now_text, "
                    "updated_at = :now_text "
                    "REMOVE processing_token, "
                    "lease_expires_at, last_error"
                ),
                ConditionExpression=(
                    "#st = :processing "
                    "AND processing_token = :token "
                    "AND lease_expires_at > :epoch"
                ),
                ExpressionAttributeNames={
                    "#st": "status",
                },
                ExpressionAttributeValues={
                    ":completed": "COMPLETED",
                    ":processing": "PROCESSING",
                    ":token": token,
                    ":epoch": int(now.timestamp()),
                    ":now_text": now.isoformat(),
                },
            )

        except ClientError as error:
            if is_condition_failure(error):
                raise BatchConflict(
                    "Cannot complete: invalid owner, "
                    "status, or expired lease."
                ) from error
            raise

    # ----------------------------------------------
    # Mark the current owner's batch as failed
    # ----------------------------------------------

    def mark_failed(
        self,
        dataset,
        batch_id,
        token,
        error_message,
    ):
        try:
            self.table.update_item(
                Key={
                    "dataset": dataset,
                    "batch_id": batch_id,
                },
                UpdateExpression=(
                    "SET #st = :failed, "
                    "last_error = :error, "
                    "updated_at = :now_text "
                    "REMOVE processing_token, "
                    "lease_expires_at"
                ),
                ConditionExpression=(
                    "#st = :processing "
                    "AND processing_token = :token"
                ),
                ExpressionAttributeNames={
                    "#st": "status",
                },
                ExpressionAttributeValues={
                    ":failed": "FAILED",
                    ":processing": "PROCESSING",
                    ":token": token,
                    ":error": error_message[:1000],
                    ":now_text": iso_now(),
                },
            )

        except ClientError as error:
            if is_condition_failure(error):
                raise BatchConflict(
                    "Cannot mark failed: "
                    "processing ownership changed."
                ) from error
            raise
