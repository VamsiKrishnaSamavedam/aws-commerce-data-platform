from datetime import timedelta
import random

import pandas as pd


# --------------------------------------------------
# Configuration
# --------------------------------------------------

RANDOM_SEED = 42

random.seed(RANDOM_SEED)


PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "paypal",
    "apple_pay",
    "gift_card"
]


# --------------------------------------------------
# Read orders
# --------------------------------------------------

orders_df = pd.read_csv(
    "data/generated/orders.csv"
)

orders_df["order_date"] = pd.to_datetime(
    orders_df["order_date"]
)


# --------------------------------------------------
# Generate payments
# --------------------------------------------------

def generate_payments(orders):

    payments = []

    payment_number = 1

    for _, order in orders.iterrows():

        payment_id = f"PAY{payment_number:07d}"

        payment_method = random.choice(
            PAYMENT_METHODS
        )

        payment_date = (
            order["order_date"]
            + timedelta(
                minutes=random.randint(1, 60)
            )
        )

        if order["status"] == "cancelled":

            payment_status = "failed"

        else:

            payment_status = "completed"

        payment = {
            "payment_id": payment_id,
            "order_id": order["order_id"],
            "payment_method": payment_method,
            "amount": order["total_amount"],
            "payment_status": payment_status,
            "payment_date": payment_date
        }

        payments.append(payment)

        payment_number += 1

    return pd.DataFrame(payments)


# --------------------------------------------------
# Generate
# --------------------------------------------------

payments_df = generate_payments(
    orders_df
)


# --------------------------------------------------
# Validation
# --------------------------------------------------

assert payments_df["payment_id"].is_unique

assert set(
    payments_df["order_id"]
).issubset(
    set(
        orders_df["order_id"]
    )
)

assert (
    payments_df["amount"] >= 0
).all()


# --------------------------------------------------
# Validate payment amount
# --------------------------------------------------

payment_check = payments_df.merge(
    orders_df[
        [
            "order_id",
            "total_amount"
        ]
    ],
    on="order_id",
    how="left"
)

assert (
    payment_check["amount"].round(2)
    ==
    payment_check["total_amount"].round(2)
).all()


# --------------------------------------------------
# Save
# --------------------------------------------------

payments_df.to_csv(
    "data/generated/payments.csv",
    index=False
)


# --------------------------------------------------
# Output
# --------------------------------------------------

print(payments_df.head())

print()

print(
    f"Generated {len(payments_df)} payments."
)

print(
    "Payment validation passed."
)