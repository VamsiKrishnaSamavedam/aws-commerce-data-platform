import unittest

from src.utils.incremental_manifest import (
    ManifestValidationError,
    validate_customer_manifest,
    watermark_batch_id,
)


BUCKET = "aws-commerce-data-platform-036071343416-us-east-1"
SHA = "82edcad1ef55bde5ca2803840b736f7bfa4312ccc6192244d8e508fd65efa536"


def manifest():
    return {
        "schema_version": 1,
        "dataset": "customers",
        "stream_id": "customers_incremental_v1",
        "sequence": 1,
        "batch_id": SHA[:16],
        "source_s3_uri": (
            f"s3://{BUCKET}/bronze/customers/incremental/"
            "year=2026/month=10/day=22/"
            f"batch_id={SHA[:16]}/customers.csv"
        ),
        "sha256": SHA,
        "row_count": 10,
        "published_at": "2026-10-22T17:51:57+00:00",
    }


class IncrementalManifestTests(unittest.TestCase):
    def test_valid_manifest_normalizes_source_key(self):
        validated = validate_customer_manifest(manifest(), BUCKET)
        self.assertEqual(validated["batch_id"], SHA[:16])
        self.assertTrue(validated["source_key"].endswith("customers.csv"))

    def test_checksum_must_match_batch_id(self):
        value = manifest()
        value["batch_id"] = "0" * 16
        with self.assertRaises(ManifestValidationError):
            validate_customer_manifest(value, BUCKET)

    def test_sequence_must_be_positive_integer(self):
        value = manifest()
        value["sequence"] = 0
        with self.assertRaises(ManifestValidationError):
            validate_customer_manifest(value, BUCKET)

    def test_watermark_key_is_reserved(self):
        self.assertEqual(
            watermark_batch_id("customers_incremental_v1"),
            "__watermark__#customers_incremental_v1",
        )


if __name__ == "__main__":
    unittest.main()
