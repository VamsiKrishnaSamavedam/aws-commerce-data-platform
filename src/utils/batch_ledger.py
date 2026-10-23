
from datetime import datetime, timezone
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeSerializer

from src.utils.incremental_manifest import watermark_batch_id


# ============================================================
# 1. CONFIGURATION
# ============================================================

TABLE_NAME = "commerce-batch-ledger"
AWS_REGION = "us-east-1"


# ============================================================
# 2. CUSTOM EXCEPTIONS
# ============================================================

class BatchConflict(RuntimeError):
    """Batch state or processing ownership conflict."""
    pass


class BatchNotClaimable(RuntimeError):
    """Batch cannot be claimed for processing."""
    pass


# ============================================================
# 3. HELPER FUNCTIONS
# ============================================================

def utc_now():
    """Return current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def iso_now():
    """Return current UTC time in ISO 8601 format."""
    return utc_now().isoformat()


def is_condition_failure(error):
    """Check for a DynamoDB conditional write failure."""
    return (
        error.response["Error"]["Code"]
        == "ConditionalCheckFailedException"
    )


# ============================================================
# 4. BATCH LEDGER CLASS
# ============================================================

class BatchLedger:

    def __init__(self):
        """
        Create a DynamoDB resource using its own session.

        Each thread should create its own BatchLedger
        instance rather than sharing a Boto3 resource.
        """

        session = boto3.session.Session(
            region_name=AWS_REGION
        )

        dynamodb = session.resource("dynamodb")

        self.table = dynamodb.Table(TABLE_NAME)
        self._serializer = TypeSerializer()

    def _wire_item(self, item):
        """Keep native values for the DynamoDB resource client's transaction API."""
        return item

    # ========================================================
    # 5. RETRIEVE A BATCH
    # ========================================================

    def get_batch(self, dataset, batch_id):
        """
        Retrieve a batch using its composite key.

        ConsistentRead=True ensures the latest committed
        DynamoDB item is returned.
        """

        response = self.table.get_item(
            Key={
                "dataset": dataset,
                "batch_id": batch_id,
            },
            ConsistentRead=True,
        )

        return response.get("Item")

    # ========================================================
    # 6. INSERT A NEW RECORD ONLY ONCE
    # ========================================================

    def _insert_once(self, item):
        """
        Insert only when the batch doesn't already exist.

        Prevents accidental replacement of an existing
        PENDING, PROCESSING, FAILED or COMPLETED item.
        """

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

    # ========================================================
    # 7. REGISTER A NEW PENDING BATCH
    # ========================================================

    def register_pending(
        self,
        dataset,
        batch_id,
        source_s3_uri,
        sha256,
    ):
        """
        Register a new batch with status PENDING.

        Existing registrations are not overwritten.
        """

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

        existing = self.get_batch(
            dataset,
            batch_id,
        )

        if existing is None:
            raise RuntimeError(
                "Registration conflict. Retry lookup."
            )

        # Verify this batch ID refers to the same source.

        if (
            existing.get("source_s3_uri")
            != source_s3_uri
            or existing.get("sha256") != sha256
        ):
            raise BatchConflict(
                "Batch already exists with different "
                "source metadata."
            )

        return "ALREADY_REGISTERED"

    # ========================================================
    # 7B. REGISTER AN IMMUTABLE, SEQUENCED MANIFEST
    # ========================================================

    def get_watermark(self, dataset, stream_id):
        """Retrieve the durable registration/completion watermark."""
        return self.get_batch(
            dataset,
            watermark_batch_id(stream_id),
        )

    def register_sequenced_pending(self, manifest):
        """Atomically register the next sequence and advance registration state.

        A stream may only register sequence N after N-1.  A duplicate event is
        harmless only when every immutable manifest field matches the existing
        ledger entry.
        """
        dataset = manifest["dataset"]
        batch_id = manifest["batch_id"]
        stream_id = manifest["stream_id"]
        sequence = manifest["sequence"]
        watermark_id = watermark_batch_id(stream_id)
        now = iso_now()

        existing = self.get_batch(dataset, batch_id)
        if existing is not None:
            immutable_fields = (
                "stream_id", "sequence", "source_s3_uri", "sha256",
                "row_count", "schema_version",
            )
            if any(existing.get(field) != manifest[field] for field in immutable_fields):
                raise BatchConflict(
                    "Existing batch conflicts with immutable manifest metadata."
                )
            return "ALREADY_REGISTERED"

        current_watermark = self.get_watermark(dataset, stream_id)
        if current_watermark is None and sequence != 1:
            raise BatchConflict(
                "The first registered sequence for a stream must be 1."
            )

        batch_item = {
            "dataset": dataset,
            "batch_id": batch_id,
            "stream_id": stream_id,
            "sequence": sequence,
            "source_s3_uri": manifest["source_s3_uri"],
            "sha256": manifest["sha256"],
            "row_count": manifest["row_count"],
            "schema_version": manifest["schema_version"],
            "manifest_published_at": manifest["published_at"],
            "status": "PENDING",
            "attempt_count": 0,
            "registration_mode": "sequenced_manifest",
            "created_at": now,
            "updated_at": now,
        }

        # The first sequence has no watermark item to update.  Create both
        # records in one transaction instead of relying on UpdateItem's
        # create-on-missing behavior, which evaluates key attributes as set.
        if current_watermark is None:
            watermark_item = {
                "dataset": dataset,
                "batch_id": watermark_id,
                "stream_id": stream_id,
                "registered_through_sequence": sequence,
                "last_registered_batch_id": batch_id,
                "last_registered_at": now,
                "last_completed_sequence": 0,
                "updated_at": now,
            }
            try:
                self.table.meta.client.transact_write_items(
                    TransactItems=[
                        {
                            "Put": {
                                "TableName": TABLE_NAME,
                                "Item": self._wire_item(batch_item),
                                "ConditionExpression": "attribute_not_exists(#pk)",
                                "ExpressionAttributeNames": {"#pk": "dataset"},
                            },
                        },
                        {
                            "Put": {
                                "TableName": TABLE_NAME,
                                "Item": self._wire_item(watermark_item),
                                "ConditionExpression": "attribute_not_exists(#pk)",
                                "ExpressionAttributeNames": {"#pk": "dataset"},
                            },
                        },
                    ]
                )
            except ClientError as error:
                existing = self.get_batch(dataset, batch_id)
                if existing is not None:
                    return "ALREADY_REGISTERED"
                raise BatchConflict(
                    "Unable to atomically create the initial stream watermark: "
                    f"{error.response.get('CancellationReasons', [])}"
                ) from error
            return "REGISTERED"

        try:
            self.table.meta.client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": TABLE_NAME,
                            "Item": self._wire_item(batch_item),
                            "ConditionExpression": (
                                "attribute_not_exists(#pk) AND "
                                "attribute_not_exists(#sk)"
                            ),
                            "ExpressionAttributeNames": {
                                "#pk": "dataset", "#sk": "batch_id",
                            },
                        },
                    },
                    {
                        "Update": {
                            "TableName": TABLE_NAME,
                            "Key": self._wire_item({
                                "dataset": dataset,
                                "batch_id": watermark_id,
                            }),
                            "UpdateExpression": (
                                "SET stream_id = :stream, "
                                "registered_through_sequence = :sequence, "
                                "last_registered_batch_id = :batch_id, "
                                "last_registered_at = :now, "
                                "last_completed_sequence = "
                                "if_not_exists(last_completed_sequence, :zero), "
                                "updated_at = :now"
                            ),
                            "ConditionExpression": (
                                "attribute_not_exists(registered_through_sequence) OR "
                                "registered_through_sequence = :previous"
                            ),
                            "ExpressionAttributeValues": self._wire_item({
                                ":stream": stream_id,
                                ":sequence": sequence,
                                ":batch_id": batch_id,
                                ":now": now,
                                ":zero": 0,
                                ":previous": sequence - 1,
                            }),
                        },
                    },
                ]
            )
        except ClientError as error:
            if not is_condition_failure(error) and error.response["Error"]["Code"] != "TransactionCanceledException":
                raise

            existing = self.get_batch(dataset, batch_id)
            if existing is not None:
                immutable_fields = (
                    "stream_id", "sequence", "source_s3_uri", "sha256",
                    "row_count", "schema_version",
                )
                if all(existing.get(field) == manifest[field] for field in immutable_fields):
                    return "ALREADY_REGISTERED"
            watermark = self.get_watermark(dataset, stream_id)
            registered = int(watermark.get("registered_through_sequence", 0)) if watermark else 0
            raise BatchConflict(
                "Sequence cannot be registered yet; expected "
                f"{registered + 1}, received {sequence}."
            ) from error

        return "REGISTERED"

    # ========================================================
    # 8. BACKFILL A HISTORICALLY COMPLETED BATCH
    # ========================================================

    def backfill_completed(
        self,
        dataset,
        batch_id,
        source_s3_uri,
        sha256,
    ):
        """
        Record a batch that was successfully processed
        before DynamoDB ledger tracking was introduced.

        Does not execute the MERGE again.
        """

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

        existing = self.get_batch(
            dataset,
            batch_id,
        )

        if existing is None:
            raise RuntimeError(
                "Backfill conflict. Retry lookup."
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
            "Cannot overwrite its status."
        )

    # ========================================================
    # 9. CONDITIONALLY CLAIM A BATCH
    # ========================================================

    def claim_batch(
        self,
        dataset,
        batch_id,
        lease_seconds=1800,
    ):
        """
        Atomically claim a batch for processing.

        Allowed:
            PENDING -> PROCESSING
            FAILED -> PROCESSING
            Expired PROCESSING -> PROCESSING

        Not allowed:
            COMPLETED -> PROCESSING
            Active PROCESSING -> PROCESSING

        Returns a unique processing ownership token.
        """

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive."
            )

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
                    "attribute_exists(#pk) AND "
                    "("
                    "#st IN (:pending, :failed) "
                    "OR "
                    "(#st = :processing AND "
                    "lease_expires_at <= :epoch)"
                    ")"
                ),
                ExpressionAttributeNames={
                    "#pk": "dataset",
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
                    "Batch is completed, actively claimed, "
                    "or not registered."
                ) from error

            raise

        return token

    def claim_sequenced_batch(
        self,
        dataset,
        batch_id,
        lease_seconds=1800,
    ):
        """Claim a manifest batch only when it is the next completion sequence."""
        record = self.get_batch(dataset, batch_id)
        if record is None:
            raise BatchNotClaimable("Batch is not registered.")
        if "sequence" not in record or "stream_id" not in record:
            raise BatchConflict("Batch is not a sequenced manifest batch.")

        watermark = self.get_watermark(dataset, record["stream_id"])
        completed = int(watermark.get("last_completed_sequence", 0)) if watermark else 0
        if int(record["sequence"]) != completed + 1:
            raise BatchNotClaimable(
                "Batch is waiting for an earlier sequence to complete."
            )

        return self.claim_batch(dataset, batch_id, lease_seconds)

    def reserve_orchestration(
        self,
        dataset,
        batch_id,
        lease_seconds=14400,
    ):
        """Reserve one eligible batch for a Step Functions execution.

        EventBridge delivers at least once.  This independent lease prevents
        duplicate events from starting duplicate Glue jobs before the Glue
        worker can take its own processing lease.
        """
        now = utc_now()
        token = str(uuid4())
        try:
            self.table.update_item(
                Key={"dataset": dataset, "batch_id": batch_id},
                UpdateExpression=(
                    "SET orchestration_token = :token, "
                    "orchestration_lease_expires_at = :expiry, "
                    "orchestration_started_at = :now, updated_at = :now"
                ),
                ConditionExpression=(
                    "(#st IN (:pending, :failed) OR "
                    "(#st = :processing AND lease_expires_at <= :epoch)) AND "
                    "(attribute_not_exists(orchestration_lease_expires_at) "
                    "OR orchestration_lease_expires_at <= :epoch "
                    "OR (#st = :processing AND lease_expires_at <= :epoch))"
                ),
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":pending": "PENDING",
                    ":failed": "FAILED",
                    ":processing": "PROCESSING",
                    ":token": token,
                    ":expiry": int(now.timestamp()) + lease_seconds,
                    ":epoch": int(now.timestamp()),
                    ":now": now.isoformat(),
                },
            )
            return token
        except ClientError as error:
            if is_condition_failure(error):
                return None
            raise

    # ========================================================
    # 10. RENEW THE PROCESSING LEASE
    # ========================================================

    def renew_lease(
        self,
        dataset,
        batch_id,
        token,
        lease_seconds=300,
    ):
        """
        Renew the lease for a running Glue job.

        Renewal succeeds only if:
            1. Status is PROCESSING.
            2. The token matches the current owner.
            3. The existing lease has not expired.

        Used by the background heartbeat.
        """

        if lease_seconds <= 0:
            raise ValueError(
                "lease_seconds must be positive."
            )

        now = utc_now()
        now_epoch = int(now.timestamp())

        new_expiry = now_epoch + lease_seconds

        try:
            self.table.update_item(
                Key={
                    "dataset": dataset,
                    "batch_id": batch_id,
                },
                UpdateExpression=(
                    "SET lease_expires_at = :expiry, "
                    "updated_at = :timestamp"
                ),
                ConditionExpression=(
                    "#st = :processing "
                    "AND processing_token = :token "
                    "AND lease_expires_at > :now"
                ),
                ExpressionAttributeNames={
                    "#st": "status",
                },
                ExpressionAttributeValues={
                    ":processing": "PROCESSING",
                    ":token": token,
                    ":expiry": new_expiry,
                    ":now": now_epoch,
                    ":timestamp": now.isoformat(),
                },
            )

        except ClientError as error:

            if is_condition_failure(error):
                raise BatchConflict(
                    "Lease renewal rejected. "
                    "Ownership may have expired or changed."
                ) from error

            raise

        return new_expiry

    # ========================================================
    # 11. VERIFY CURRENT PROCESSING OWNERSHIP
    # ========================================================

    def assert_owned(
        self,
        dataset,
        batch_id,
        token,
    ):
        """
        Confirm that the caller still owns an active batch.

        This method uses a strongly consistent DynamoDB read.

        It is useful before executing an Iceberg MERGE
        and before recording completion.
        """

        record = self.get_batch(
            dataset,
            batch_id,
        )

        if record is None:
            raise BatchConflict(
                "Batch does not exist."
            )

        now_epoch = int(
            utc_now().timestamp()
        )

        status = record.get("status")

        current_token = record.get(
            "processing_token"
        )

        lease_expiry = int(
            record.get("lease_expires_at", 0)
        )

        if status != "PROCESSING":
            raise BatchConflict(
                f"Batch is not PROCESSING. "
                f"Current status: {status}"
            )

        if current_token != token:
            raise BatchConflict(
                "Processing ownership token mismatch."
            )

        if lease_expiry <= now_epoch:
            raise BatchConflict(
                "Processing lease has expired."
            )

        return True

    # ========================================================
    # 12. MARK THE BATCH AS COMPLETED
    # ========================================================

    def mark_completed(
        self,
        dataset,
        batch_id,
        token,
    ):
        """
        Mark a batch COMPLETED only if this caller
        owns its current unexpired processing lease.

        Removes the ownership token and lease.
        """

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
                    "Cannot mark batch completed: "
                    "invalid owner, status, or expired lease."
                ) from error

            raise

    def mark_sequenced_completed(
        self,
        dataset,
        batch_id,
        token,
    ):
        """Complete a batch and advance its completion watermark atomically."""
        record = self.get_batch(dataset, batch_id)
        if record is None:
            raise BatchConflict("Batch does not exist.")
        if "sequence" not in record or "stream_id" not in record:
            return self.mark_completed(dataset, batch_id, token)

        now = utc_now()
        sequence = int(record["sequence"])
        watermark_id = watermark_batch_id(record["stream_id"])

        try:
            self.table.meta.client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": TABLE_NAME,
                            "Key": self._wire_item({
                                "dataset": dataset, "batch_id": batch_id,
                            }),
                            "UpdateExpression": (
                                "SET #st = :completed, "
                                "completed_at = :now_text, "
                                "updated_at = :now_text "
                                "REMOVE processing_token, lease_expires_at, last_error, "
                                "orchestration_token, orchestration_lease_expires_at"
                            ),
                            "ConditionExpression": (
                                "#st = :processing AND processing_token = :token "
                                "AND lease_expires_at > :epoch"
                            ),
                            "ExpressionAttributeNames": {"#st": "status"},
                            "ExpressionAttributeValues": self._wire_item({
                                ":completed": "COMPLETED",
                                ":processing": "PROCESSING",
                                ":token": token,
                                ":epoch": int(now.timestamp()),
                                ":now_text": now.isoformat(),
                            }),
                        },
                    },
                    {
                        "Update": {
                            "TableName": TABLE_NAME,
                            "Key": self._wire_item({
                                "dataset": dataset, "batch_id": watermark_id,
                            }),
                            "UpdateExpression": (
                                "SET last_completed_sequence = :sequence, "
                                "last_completed_batch_id = :batch_id, "
                                "last_completed_at = :now_text, updated_at = :now_text"
                            ),
                            "ConditionExpression": (
                                "last_completed_sequence = :previous"
                            ),
                            "ExpressionAttributeValues": self._wire_item({
                                ":sequence": sequence,
                                ":previous": sequence - 1,
                                ":batch_id": batch_id,
                                ":now_text": now.isoformat(),
                            }),
                        },
                    },
                ]
            )
        except ClientError as error:
            if is_condition_failure(error) or error.response["Error"]["Code"] == "TransactionCanceledException":
                raise BatchConflict(
                    "Cannot atomically complete sequenced batch; ownership or "
                    "watermark order changed."
                ) from error
            raise

    # ========================================================
    # 13. MARK THE BATCH AS FAILED
    # ========================================================

    def mark_failed(
        self,
        dataset,
        batch_id,
        token,
        error_message,
    ):
        """
        Mark a batch FAILED when processing fails.

        The caller must own the current unexpired lease.

        Do not call this automatically when an Iceberg
        MERGE might already have committed.
        """

        now = utc_now()

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
                    "AND processing_token = :token "
                    "AND lease_expires_at > :epoch"
                ),
                ExpressionAttributeNames={
                    "#st": "status",
                },
                ExpressionAttributeValues={
                    ":failed": "FAILED",
                    ":processing": "PROCESSING",
                    ":token": token,
                    ":epoch": int(now.timestamp()),
                    ":error": str(error_message)[:1000],
                    ":now_text": now.isoformat(),
                },
            )

        except ClientError as error:

            if is_condition_failure(error):
                raise BatchConflict(
                    "Cannot mark batch failed: "
                    "invalid owner, status, or expired lease."
                ) from error

            raise
