from datetime import datetime

import pandas as pd


# --------------------------------------------------
# Paths
# --------------------------------------------------

SOURCE_PATH = "data/generated/customers.csv"

OUTPUT_PATH = "data/generated/customers_dirty.csv"


# --------------------------------------------------
# Read existing good customers
# --------------------------------------------------

customers_df = pd.read_csv(
    SOURCE_PATH
)


# --------------------------------------------------
# Invalid test records
# --------------------------------------------------

bad_records = [
    {
        "customer_id": None,
        "first_name": "Missing",
        "last_name": "CustomerId",
        "email": "missing.id@example.com",
        "country": "United States",
        "created_at": "2026-01-01 10:00:00",
        "updated_at": "2026-01-02 10:00:00",
    },

    {
        "customer_id": "C900001",
        "first_name": "   ",
        "last_name": "BlankName",
        "email": "blank.name@example.com",
        "country": "United States",
        "created_at": "2026-01-01 10:00:00",
        "updated_at": "2026-01-02 10:00:00",
    },

    {
        "customer_id": "C900002",
        "first_name": "No",
        "last_name": "Email",
        "email": None,
        "country": "United States",
        "created_at": "2026-01-01 10:00:00",
        "updated_at": "2026-01-02 10:00:00",
    },

    {
        "customer_id": "C900003",
        "first_name": "Broken",
        "last_name": "Timestamp",
        "email": "broken.timestamp@example.com",
        "country": "United States",
        "created_at": "not-a-date",
        "updated_at": "2026-01-02 10:00:00",
    },

    {
        "customer_id": "C900004",
        "first_name": "Wrong",
        "last_name": "DateOrder",
        "email": "wrong.date@example.com",
        "country": "United States",
        "created_at": "2026-05-10 10:00:00",
        "updated_at": "2026-05-01 10:00:00",
    },
]


bad_df = pd.DataFrame(
    bad_records
)


# --------------------------------------------------
# Create one duplicate customer
# --------------------------------------------------

duplicate_customer = (
    customers_df.iloc[0].copy()
)

duplicate_customer[
    "first_name"
] = (
    f"   {duplicate_customer['first_name']}   "
)

duplicate_customer[
    "email"
] = (
    f"   {duplicate_customer['email'].upper()}   "
)

duplicate_customer[
    "updated_at"
] = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)


duplicate_df = pd.DataFrame(
    [duplicate_customer]
)


# --------------------------------------------------
# Combine everything
# --------------------------------------------------

dirty_customers_df = pd.concat(
    [
        customers_df,
        bad_df,
        duplicate_df,
    ],
    ignore_index=True,
)


# --------------------------------------------------
# Save test batch
# --------------------------------------------------

dirty_customers_df.to_csv(
    OUTPUT_PATH,
    index=False,
)


# --------------------------------------------------
# Output
# --------------------------------------------------

print(
    f"Original rows: {len(customers_df)}"
)

print(
    f"Bad rows added: {len(bad_df)}"
)

print(
    f"Duplicate rows added: {len(duplicate_df)}"
)

print(
    f"Final rows: {len(dirty_customers_df)}"
)

print(
    f"Created: {OUTPUT_PATH}"
)