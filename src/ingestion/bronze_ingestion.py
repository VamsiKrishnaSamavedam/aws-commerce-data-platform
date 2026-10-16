from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import logging
import os

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError
)
from dotenv import load_dotenv
load_dotenv()

# LOAD CONFIGURATION FROM ENVIRONMENT VARIABLES

BUCKET_NAME = os.getenv(
    "COMMERCE_S3_BUCKET"
)

AWS_REGION = os.getenv(
    "AWS_REGION",
    "us-east-1"
)


# --------------------------------------------------
#Dataset configuration

DATASETS = {
    "customers": Path(
        "data/generated/customers.csv"
    ),

    "products": Path(
        "data/generated/products.csv"
    ),

    "orders": Path(
        "data/generated/orders.csv"
    ),

    "order_items": Path(
        "data/generated/order_items.csv"
    ),

    "payments": Path(
        "data/generated/payments.csv"
    ),

    "shipments": Path(
        "data/generated/shipments.csv"
    ),

    "returns": Path(
        "data/generated/returns.csv"
    )
}

#--------------------------------------------------
# LOGGING CONFIGURATION
logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )
)

logger = logging.getLogger(
    __name__
)
logger.info(
    "Upload successful"
)

#--------------------------------------------------
# SHA-256 Hashing Function
def calculate_sha256(file_path):
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:

        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b""
        ):
            sha256.update(chunk)

    return sha256.hexdigest()

#--------------------------------------------------
# FILE VALIDATION FUNCTION

def validate_local_file(
    dataset_name,
    file_path
):

    if not file_path.exists():
        raise FileNotFoundError(
            f"{dataset_name}: "
            f"file not found: {file_path}"
        )

    if not file_path.is_file():
        raise ValueError(
            f"{dataset_name}: "
            f"path is not a file."
        )

    if file_path.stat().st_size == 0:
        raise ValueError(
            f"{dataset_name}: "
            f"file is empty."
        )

#--------------------------------------------------
# BUILD BRONZE KEY FUNCTION
def build_bronze_key(
    dataset_name,
    file_name,
    batch_id,
    ingestion_time
):

    return (
        f"bronze/{dataset_name}/"
        f"year={ingestion_time:%Y}/"
        f"month={ingestion_time:%m}/"
        f"day={ingestion_time:%d}/"
        f"batch_id={batch_id}/"
        f"{file_name}"
    )

#--------------------------------------------------
# BUILD MANIFESTATION FUNCTION

def build_manifest_key(
    dataset_name,
    batch_id
):

    return (
        f"bronze/_manifests/"
        f"{dataset_name}/"
        f"{batch_id}.json"
    )

#--------------------------------------------------
# CHECKING WHETHER THE BATCH ALREADY EXISTS IN S3

def manifest_exists(
    s3_client,
    bucket_name,
    manifest_key
):

    try:

        s3_client.head_object(
            Bucket=bucket_name,
            Key=manifest_key
        )

        return True

    except ClientError as error:

        error_code = (
            error.response
            .get("Error", {})
            .get("Code")
        )

        if error_code in {
            "404",
            "NoSuchKey",
            "NotFound"
        }:
            return False

        raise


#--------------------------------------------------
# UPLOAD VERIFICATION

def verify_upload(
    s3_client,
    bucket_name,
    object_key,
    expected_size,
    expected_checksum
):

    response = s3_client.head_object(
        Bucket=bucket_name,
        Key=object_key
    )

    actual_size = response[
        "ContentLength"
    ]

    metadata = response.get(
        "Metadata",
        {}
    )

    actual_checksum = metadata.get(
        "sha256"
    )

    if actual_size != expected_size:
        raise ValueError(
            "Uploaded file size does not "
            "match local file size."
        )

    if actual_checksum != expected_checksum:
        raise ValueError(
            "Uploaded checksum metadata "
            "does not match."
        )

#--------------------------------------------------
# MAIN DATASET UPLOADER


def upload_dataset(
    s3_client,
    bucket_name,
    dataset_name,
    file_path
):

    logger.info(
        "Starting dataset: %s",
        dataset_name
    )


    # ----------------------------------------------
    # Validate local source file
    # ----------------------------------------------

    validate_local_file(
        dataset_name,
        file_path
    )


    # ----------------------------------------------
    # File metadata
    # ----------------------------------------------

    file_size = (
        file_path
        .stat()
        .st_size
    )

    checksum = calculate_sha256(
        file_path
    )

    batch_id = checksum[:16]


    # ----------------------------------------------
    # Check idempotency
    # ----------------------------------------------

    manifest_key = build_manifest_key(
        dataset_name,
        batch_id
    )

    if manifest_exists(
        s3_client,
        bucket_name,
        manifest_key
    ):

        logger.info(
            "%s | batch %s already "
            "ingested. Skipping.",
            dataset_name,
            batch_id
        )

        return "skipped"


    # ----------------------------------------------
    # Ingestion time
    # ----------------------------------------------

    ingestion_time = datetime.now(
        timezone.utc
    )


    # ----------------------------------------------
    # Build data object key
    # ----------------------------------------------

    object_key = build_bronze_key(
        dataset_name=dataset_name,
        file_name=file_path.name,
        batch_id=batch_id,
        ingestion_time=ingestion_time
    )


    # ----------------------------------------------
    # Upload
    # ----------------------------------------------

    logger.info(
        "%s | uploading batch %s",
        dataset_name,
        batch_id
    )


    s3_client.upload_file(
        str(file_path),
        bucket_name,
        object_key,
        ExtraArgs={
            "Metadata": {
                "dataset": dataset_name,
                "batch-id": batch_id,
                "sha256": checksum
            }
        }
    )


    # ----------------------------------------------
    # Verify upload
    # ----------------------------------------------

    verify_upload(
        s3_client=s3_client,
        bucket_name=bucket_name,
        object_key=object_key,
        expected_size=file_size,
        expected_checksum=checksum
    )


    # ----------------------------------------------
    # Create manifest
    # ----------------------------------------------

    manifest = {
        "dataset": dataset_name,
        "batch_id": batch_id,
        "source_file": file_path.name,
        "file_size_bytes": file_size,
        "checksum_sha256": checksum,
        "ingested_at": (
            ingestion_time.isoformat()
        ),
        "s3_bucket": bucket_name,
        "s3_key": object_key
    }


    s3_client.put_object(
        Bucket=bucket_name,
        Key=manifest_key,
        Body=json.dumps(
            manifest,
            indent=2
        ).encode("utf-8"),
        ContentType="application/json"
    )


    logger.info(
        "%s | batch %s uploaded "
        "and verified.",
        dataset_name,
        batch_id
    )

    return "uploaded"

#--------------------------------------------------
# CREATE MAIN INGESTION FLOW FUNCTION

def ingest_all():

    if not BUCKET_NAME:
        raise ValueError(
            "COMMERCE_S3_BUCKET is not set."
        )


    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION
    )


    uploaded = []
    skipped = []
    failed = []


    for dataset_name, file_path in DATASETS.items():

        try:

            result = upload_dataset(
                s3_client=s3_client,
                bucket_name=BUCKET_NAME,
                dataset_name=dataset_name,
                file_path=file_path
            )


            if result == "uploaded":
                uploaded.append(
                    dataset_name
                )

            elif result == "skipped":
                skipped.append(
                    dataset_name
                )


        except (
            NoCredentialsError,
            BotoCoreError,
            ClientError,
            FileNotFoundError,
            ValueError
        ) as error:

            logger.exception(
                "%s | ingestion failed: %s",
                dataset_name,
                error
            )

            failed.append(
                dataset_name
            )


    logger.info(
        "Ingestion summary | "
        "uploaded=%s | "
        "skipped=%s | "
        "failed=%s",
        len(uploaded),
        len(skipped),
        len(failed)
    )


    if failed:

        raise RuntimeError(
            "Ingestion failed for: "
            + ", ".join(failed)
        )

if __name__ == "__main__":
    ingest_all()