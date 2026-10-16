from datetime import datetime, timedelta
import random

import pandas as pd


# --------------------------------------------------
# Configuration
# --------------------------------------------------

RANDOM_SEED = 42

random.seed(RANDOM_SEED)


CARRIERS = [
    "UPS",
    "FedEx",
    "USPS",
    "DHL"
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
# Generator
# --------------------------------------------------

def generate_shipments(orders):

    shipments = []

    shipment_number = 1

    now = datetime.now()

    for _, order in orders.iterrows():

        if order["status"] not in [
            "completed",
            "shipped"
        ]:
            continue

        ship_date = (
            order["order_date"]
            + timedelta(
                days=random.randint(1, 3)
            )
        )

        if ship_date > now:
            continue

        carrier = random.choice(
            CARRIERS
        )


        if order["status"] == "completed":

            delivery_date = (
                ship_date
                + timedelta(
                    days=random.randint(1, 7)
                )
            )

            if delivery_date > now:
                delivery_date = now

            shipment_status = "delivered"

        else:

            delivery_date = None

            shipment_status = "shipped"


        shipment = {
            "shipment_id":
                f"SHP{shipment_number:07d}",

            "order_id":
                order["order_id"],

            "carrier":
                carrier,

            "ship_date":
                ship_date,

            "delivery_date":
                delivery_date,

            "status":
                shipment_status
        }

        shipments.append(
            shipment
        )

        shipment_number += 1

    return pd.DataFrame(
        shipments
    )


# --------------------------------------------------
# Generate
# --------------------------------------------------

shipments_df = generate_shipments(
    orders_df
)


# --------------------------------------------------
# Validation
# --------------------------------------------------

assert shipments_df[
    "shipment_id"
].is_unique


assert set(
    shipments_df["order_id"]
).issubset(
    set(
        orders_df["order_id"]
    )
)


shipment_check = shipments_df.merge(
    orders_df[
        [
            "order_id",
            "order_date"
        ]
    ],
    on="order_id",
    how="left"
)


shipment_check["ship_date"] = pd.to_datetime(
    shipment_check["ship_date"]
)

shipment_check["delivery_date"] = pd.to_datetime(
    shipment_check["delivery_date"]
)


assert (
    shipment_check["ship_date"]
    >=
    shipment_check["order_date"]
).all()


delivered = shipment_check[
    shipment_check[
        "delivery_date"
    ].notna()
]


assert (
    delivered["delivery_date"]
    >=
    delivered["ship_date"]
).all()


# --------------------------------------------------
# Save
# --------------------------------------------------

shipments_df.to_csv(
    "data/generated/shipments.csv",
    index=False
)


# --------------------------------------------------
# Output
# --------------------------------------------------

print(shipments_df.head())

print()

print(
    f"Generated {len(shipments_df)} shipments."
)

print(
    "Shipment validation passed."
)