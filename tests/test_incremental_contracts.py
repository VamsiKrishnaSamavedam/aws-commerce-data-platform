import unittest

from src.utils.incremental_datasets import DATASETS, contract
from src.utils.incremental_manifest import ManifestValidationError, validate_dataset_manifest


BUCKET = "aws-commerce-data-platform-036071343416-us-east-1"


def manifest(dataset="products", sequence=1):
    batch_id = "a" * 16
    return {
        "schema_version": 1,
        "dataset": dataset,
        "stream_id": f"{dataset}_incremental_v1",
        "sequence": sequence,
        "batch_id": batch_id,
        "source_s3_uri": (
            f"s3://{BUCKET}/bronze/{dataset}/incremental/year=2026/month=10/day=22/"
            f"batch_id={batch_id}/{dataset}.csv"
        ),
        "sha256": batch_id + "b" * 48,
        "row_count": 1,
        "published_at": "2026-10-22T00:00:00+00:00",
    }


class IncrementalContractTests(unittest.TestCase):
    def test_every_incremental_dataset_has_a_contract(self):
        for dataset in ("products", "orders", "order_items", "payments", "shipments", "returns"):
            self.assertIn("key", contract(dataset))
            self.assertIn("columns", contract(dataset))
            self.assertIn("mode", contract(dataset))

    def test_append_only_and_upsert_choices_are_explicit(self):
        self.assertEqual(DATASETS["order_items"]["mode"], "append_only")
        self.assertEqual(DATASETS["payments"]["mode"], "append_only")
        self.assertEqual(DATASETS["returns"]["mode"], "append_only")
        self.assertEqual(DATASETS["products"]["mode"], "upsert")
        self.assertEqual(DATASETS["orders"]["mode"], "upsert")
        self.assertEqual(DATASETS["shipments"]["mode"], "upsert")

    def test_valid_dataset_manifest_is_normalized(self):
        value = validate_dataset_manifest(manifest(), BUCKET)
        self.assertEqual(value["source_key"].split("/")[1], "products")

    def test_manifest_rejects_wrong_path_dataset(self):
        value = manifest("orders")
        value["source_s3_uri"] = value["source_s3_uri"].replace("/orders.csv", "/products.csv")
        with self.assertRaises(ManifestValidationError):
            validate_dataset_manifest(value, BUCKET)

    def test_manifest_rejects_nonpositive_row_count(self):
        value = manifest()
        value["row_count"] = 0
        with self.assertRaises(ManifestValidationError):
            validate_dataset_manifest(value, BUCKET)


if __name__ == "__main__":
    unittest.main()
