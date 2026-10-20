
import argparse
import hashlib
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import boto3
from dotenv import load_dotenv

from src.utils.batch_ledger import BatchLedger


load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")

SOURCE_URI = os.getenv("CUSTOMERS_INCREMENTAL_SOURCE")

LOCAL_FILE = Path(
    "data/generated/customers_incremental_001.csv"
)


def calculate_local_sha256(path):
    checksum = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            checksum.update(chunk)

    return checksum.hexdigest()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--confirm-iceberg-verified",
        action="store_true",
        help=(
            "Confirm this batch was already merged "
            "and verified in Athena."
        ),
    )

    arguments = parser.parse_args()

    if not arguments.confirm_iceberg_verified:
        parser.error(
            "Historical completion requires explicit "
            "--confirm-iceberg-verified"
        )

    if not SOURCE_URI:
        raise ValueError(
            "CUSTOMERS_INCREMENTAL_SOURCE is missing."
        )

    if not LOCAL_FILE.exists():
        raise FileNotFoundError(LOCAL_FILE)

    # ------------------------------------------
    # Parse S3 URI
    # ------------------------------------------

    parsed = urlparse(SOURCE_URI)

    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("Invalid S3 URI")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if not (
        key.startswith("bronze/customers/")
        and key.endswith("/customers.csv")
    ):
        raise ValueError(
            "This is not a customer Bronze batch."
        )

    # ------------------------------------------
    # Extract batch ID
    # ------------------------------------------

    match = re.search(
        r"(?:^|/)batch_id=([0-9a-f]{16})/",
        key,
    )

    if not match:
        raise ValueError(
            "Cannot extract batch_id from S3 key."
        )

    batch_id = match.group(1)

    # ------------------------------------------
    # Verify local file checksum
    # ------------------------------------------

    local_hash = calculate_local_sha256(
        LOCAL_FILE
    )

    if batch_id != local_hash[:16]:
        raise ValueError(
            "Batch ID does not match local SHA-256."
        )

    # ------------------------------------------
    # Independently verify S3 object bytes
    # ------------------------------------------

    s3 = boto3.client(
        "s3",
        region_name=REGION,
    )

    response = s3.get_object(
        Bucket=bucket,
        Key=key,
    )

    remote_checksum = hashlib.sha256()

    for chunk in response["Body"].iter_chunks(
        chunk_size=1024 * 1024
    ):
        if chunk:
            remote_checksum.update(chunk)

    remote_hash = remote_checksum.hexdigest()

    if remote_hash != local_hash:
        raise ValueError(
            "Local and S3 SHA-256 checksums differ."
        )

    metadata = response.get("Metadata", {})

    if metadata.get("sha256") != local_hash:
        raise ValueError(
            "S3 metadata SHA-256 does not match."
        )

    if metadata.get("batch-id") != batch_id:
        raise ValueError(
            "S3 metadata batch ID does not match."
        )

    if metadata.get("dataset") != "customers":
        raise ValueError(
            "S3 dataset metadata is incorrect."
        )

    if response["ContentLength"] != LOCAL_FILE.stat().st_size:
        raise ValueError(
            "Local and S3 file sizes differ."
        )

    print("S3 checksum verification PASSED")

    # ------------------------------------------
    # Register historical completed batch
    # ------------------------------------------

    ledger = BatchLedger()

    result = ledger.backfill_completed(
        dataset="customers",
        batch_id=batch_id,
        source_s3_uri=SOURCE_URI,
        sha256=local_hash,
    )

    print("Registration result:", result)

    # ------------------------------------------
    # Read the DynamoDB record
    # ------------------------------------------

    record = ledger.get_batch(
        "customers",
        batch_id,
    )

    print("Dataset:", record["dataset"])
    print("Batch ID:", record["batch_id"])
    print("Status:", record["status"])
    print("Registration:", record.get("registration_mode"))


if __name__ == "__main__":
    main()
