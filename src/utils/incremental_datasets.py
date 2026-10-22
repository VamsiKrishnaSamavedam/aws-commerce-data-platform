"""Dataset contracts for the Phase 7 incremental commerce pipelines."""

DATASETS = {
    "customers": {
        "key": "customer_id",
        "columns": (
            "customer_id", "first_name", "last_name", "email", "country",
            "created_at", "updated_at",
        ),
        "mode": "upsert",
        "timestamp": "updated_at",
        "references": (),
    },
    "products": {
        "key": "product_id",
        "columns": ("product_id", "product_name", "category", "price", "updated_at"),
        "mode": "upsert",
        "timestamp": "updated_at",
        "references": (),
    },
    "orders": {
        "key": "order_id",
        "columns": ("order_id", "customer_id", "order_date", "status", "total_amount", "updated_at"),
        "mode": "upsert",
        "timestamp": "updated_at",
        "references": (("customer_id", "customers"),),
    },
    "order_items": {
        "key": "order_item_id",
        "columns": ("order_item_id", "order_id", "product_id", "quantity", "unit_price"),
        "mode": "append_only",
        "timestamp": None,
        "references": (("order_id", "orders"), ("product_id", "products")),
    },
    "payments": {
        "key": "payment_id",
        "columns": ("payment_id", "order_id", "payment_method", "amount", "payment_status", "payment_date"),
        "mode": "append_only",
        "timestamp": None,
        "references": (("order_id", "orders"),),
    },
    "shipments": {
        "key": "shipment_id",
        "columns": ("shipment_id", "order_id", "carrier", "ship_date", "delivery_date", "status", "updated_at"),
        "mode": "upsert",
        "timestamp": "updated_at",
        "references": (("order_id", "orders"),),
    },
    "returns": {
        "key": "return_id",
        "columns": ("return_id", "order_id", "product_id", "return_reason", "return_date"),
        "mode": "append_only",
        "timestamp": None,
        "references": (("order_id", "orders"), ("product_id", "products")),
    },
}


def contract(dataset):
    """Return one supported dataset's immutable pipeline contract."""
    try:
        return DATASETS[dataset]
    except KeyError as error:
        raise ValueError(f"Unsupported incremental dataset: {dataset}") from error
