from datetime import datetime, timezone
from pathlib import Path
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from dotenv import load_dotenv
load_dotenv()

# --------------------------------------------------
# Configuration
# --------------------------------------------------

LOCAL_FILE = Path(
    "data/generated/customers.csv"
)

DATASET_NAME = "customers"


# --------------------------------------------------
# Build Bronze S3 object key
# --------------------------------------------------

def build_bronze_key(
    dataset_name,
    file_name,
    ingestion_time
):
    return (
        f"bronze/{dataset_name}/"
        f"year={ingestion_time:%Y}/"
        f"month={ingestion_time:%m}/"
        f"day={ingestion_time:%d}/"
        f"{file_name}"
    )


# --------------------------------------------------
# Upload
# --------------------------------------------------

def upload_to_s3():

    bucket_name = os.getenv(
        "COMMERCE_S3_BUCKET"
    )

    region = os.getenv(
        "AWS_REGION",
        "us-east-1"
    )

    if not bucket_name:
        raise ValueError(
            "COMMERCE_S3_BUCKET environment variable is not set."
        )

    if not LOCAL_FILE.exists():
        raise FileNotFoundError(
            f"Source file not found: {LOCAL_FILE}"
        )

    ingestion_time = datetime.now(
        timezone.utc
    )

    object_key = build_bronze_key(
        dataset_name=DATASET_NAME,
        file_name=LOCAL_FILE.name,
        ingestion_time=ingestion_time
    )

    s3_client = boto3.client(
        "s3",
        region_name=region
    )

    try:

        s3_client.upload_file(
            str(LOCAL_FILE),
            bucket_name,
            object_key
        )

    except NoCredentialsError as error:

        print(
            "AWS credentials were not found."
        )

        raise error

    except (ClientError, BotoCoreError) as error:

        print(
            f"S3 upload failed: {error}"
        )

        raise error

    print(
        "Upload successful."
    )

    print(
        f"s3://{bucket_name}/{object_key}"
    )


# --------------------------------------------------
# Program entry point
# --------------------------------------------------

if __name__ == "__main__":
    upload_to_s3()