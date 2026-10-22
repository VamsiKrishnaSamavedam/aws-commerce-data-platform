"""Validation helpers for immutable incremental-batch manifests."""

import re
from datetime import date
from urllib.parse import urlparse


class ManifestValidationError(ValueError):
    """Raised when an incremental manifest is not safe to register."""


CUSTOMER_SOURCE_RE = re.compile(
    r"^bronze/customers/incremental/"
    r"year=(\d{4})/month=(\d{2})/day=(\d{2})/"
    r"batch_id=([0-9a-f]{16})/customers\.csv$"
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def watermark_batch_id(stream_id):
    """Return a reserved ledger key that cannot be a content hash."""
    return f"__watermark__#{stream_id}"


def validate_customer_manifest(manifest, bucket):
    """Validate and normalize a version-1 customer batch manifest.

    Validation deliberately binds sequence, source path, batch ID, and checksum
    before the manifest is allowed to influence DynamoDB state.
    """
    if not isinstance(manifest, dict):
        raise ManifestValidationError("Manifest must be a JSON object")

    required = {
        "schema_version", "dataset", "stream_id", "sequence", "batch_id",
        "source_s3_uri", "sha256", "row_count", "published_at",
    }
    missing = required.difference(manifest)
    if missing:
        raise ManifestValidationError(
            "Manifest is missing: " + ", ".join(sorted(missing))
        )

    if manifest["schema_version"] != 1:
        raise ManifestValidationError("Unsupported manifest schema version")
    if manifest["dataset"] != "customers":
        raise ManifestValidationError("Unexpected manifest dataset")
    if not isinstance(manifest["stream_id"], str) or not manifest["stream_id"]:
        raise ManifestValidationError("Invalid stream ID")
    if (
        isinstance(manifest["sequence"], bool)
        or not isinstance(manifest["sequence"], int)
        or manifest["sequence"] < 1
    ):
        raise ManifestValidationError("Sequence must be a positive integer")
    if not SHA256_RE.fullmatch(manifest["sha256"]):
        raise ManifestValidationError("Invalid SHA-256")
    if manifest["sha256"][:16] != manifest["batch_id"]:
        raise ManifestValidationError("Batch ID/checksum mismatch")
    if (
        isinstance(manifest["row_count"], bool)
        or not isinstance(manifest["row_count"], int)
        or manifest["row_count"] < 1
    ):
        raise ManifestValidationError("Row count must be a positive integer")

    parsed = urlparse(manifest["source_s3_uri"])
    if parsed.scheme != "s3" or parsed.netloc != bucket:
        raise ManifestValidationError("Unexpected source bucket")

    key = parsed.path.lstrip("/")
    match = CUSTOMER_SOURCE_RE.fullmatch(key)
    if not match:
        raise ManifestValidationError("Unexpected customer source path")

    year, month, day, path_batch_id = match.groups()
    try:
        date(int(year), int(month), int(day))
    except ValueError as error:
        raise ManifestValidationError("Invalid source date partition") from error
    if path_batch_id != manifest["batch_id"]:
        raise ManifestValidationError("Batch ID differs from source path")

    return {
        "schema_version": manifest["schema_version"],
        "dataset": manifest["dataset"],
        "stream_id": manifest["stream_id"],
        "sequence": manifest["sequence"],
        "batch_id": manifest["batch_id"],
        "source_s3_uri": manifest["source_s3_uri"],
        "source_key": key,
        "sha256": manifest["sha256"],
        "row_count": manifest["row_count"],
        "published_at": manifest["published_at"],
    }
