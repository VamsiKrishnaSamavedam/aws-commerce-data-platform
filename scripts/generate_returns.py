from datetime import datetime, timedelta
import random

import pandas as pd


# --------------------------------------------------
# Configuration
# --------------------------------------------------

RANDOM_SEED = 42
RETURN_PROBABILITY = 0.10

random.seed(RANDOM_SEED)


RETURN_REASONS = [
    "damaged",
    "wrong_item",
    "not_as_described",
    "changed_mind",
    "defective",
    "late_delivery"
]


# --------------------------------------------------
# Read source data
# --------------------------------------------------

orders_df = pd.read_csv(
    "data/generated/orders.csv"
)

order_items_df = pd.read_csv(
    "data/generated/order_items.csv"
)

shipments_df = pd.read_csv(
    "data/generated/shipments.csv"
)


orders_df["order_date"] = pd.to_datetime(
    orders_df["order_date"]
)

shipments_df["delivery_date"] = pd.to_datetime(
    shipments_df["delivery_date"]
)


# --------------------------------------------------
# Generator
# --------------------------------------------------

def generate_returns(
    orders,
    order_items,
    shipments
):

    returns = []

    return_number = 1

    now = datetime.now()

    completed_orders = orders[
        orders["status"] == "completed"
    ]


    shipment_lookup = (
        shipments
        .set_index("order_id")
        ["delivery_date"]
        .to_dict()
    )


    for _, order in completed_orders.iterrows():

        if random.random() > RETURN_PROBABILITY:
            continue


        order_id = order["order_id"]


        if order_id not in shipment_lookup:
            continue


        delivery_date = shipment_lookup[
            order_id
        ]


        if pd.isna(delivery_date):
            continue


        items_for_order = order_items[
            order_items["order_id"]
            ==
            order_id
        ]


        if items_for_order.empty:
            continue


        selected_item = (
            items_for_order
            .sample(
                n=1,
                random_state=random.randint(
                    1,
                    100000
                )
            )
            .iloc[0]
        )


        return_date = (
            delivery_date
            + timedelta(
                days=random.randint(
                    1,
                    30
                )
            )
        )


        if return_date > now:
            return_date = now


        return_record = {
            "return_id":
                f"RET{return_number:07d}",

            "order_id":
                order_id,

            "product_id":
                selected_item[
                    "product_id"
                ],

            "return_reason":
                random.choice(
                    RETURN_REASONS
                ),

            "return_date":
                return_date
        }


        returns.append(
            return_record
        )

        return_number += 1


    return pd.DataFrame(
        returns
    )


# --------------------------------------------------
# Generate
# --------------------------------------------------

returns_df = generate_returns(
    orders_df,
    order_items_df,
    shipments_df
)


# --------------------------------------------------
# Validation
# --------------------------------------------------

assert returns_df[
    "return_id"
].is_unique


assert set(
    returns_df["order_id"]
).issubset(
    set(
        orders_df["order_id"]
    )
)


assert set(
    returns_df["product_id"]
).issubset(
    set(
        order_items_df["product_id"]
    )
)


# --------------------------------------------------
# Validate order-product combinations
# --------------------------------------------------

valid_order_products = set(
    zip(
        order_items_df["order_id"],
        order_items_df["product_id"]
    )
)


returned_order_products = set(
    zip(
        returns_df["order_id"],
        returns_df["product_id"]
    )
)


assert returned_order_products.issubset(
    valid_order_products
)


# --------------------------------------------------
# Validate return dates
# --------------------------------------------------

return_check = returns_df.merge(
    shipments_df[
        [
            "order_id",
            "delivery_date"
        ]
    ],
    on="order_id",
    how="left"
)


return_check["return_date"] = pd.to_datetime(
    return_check["return_date"]
)

return_check["delivery_date"] = pd.to_datetime(
    return_check["delivery_date"]
)


assert (
    return_check["return_date"]
    >=
    return_check["delivery_date"]
).all()


# --------------------------------------------------
# Save
# --------------------------------------------------

returns_df.to_csv(
    "data/generated/returns.csv",
    index=False
)


# --------------------------------------------------
# Output
# --------------------------------------------------

print(returns_df.head())

print()

print(
    f"Generated {len(returns_df)} returns."
)

print(
    "Return validation passed."
)