from datetime import datetime, timedelta
import random

import pandas as pd


# --------------------------------------------------
# Configuration
# --------------------------------------------------

NUMBER_OF_ORDERS = 3000
RANDOM_SEED = 42

random.seed(RANDOM_SEED)


# --------------------------------------------------
# Read parent datasets
# --------------------------------------------------

customers_df = pd.read_csv(
    "data/generated/customers.csv"
)

products_df = pd.read_csv(
    "data/generated/products.csv"
)


# Convert customer timestamps from strings
# back into datetime values.

customers_df["created_at"] = pd.to_datetime(
    customers_df["created_at"]
)


# Convert DataFrames into lists of dictionaries.

customer_records = customers_df.to_dict(
    orient="records"
)

product_records = products_df.to_dict(
    orient="records"
)


# --------------------------------------------------
# Generator
# --------------------------------------------------

def generate_orders(number_of_orders):

    orders = []
    order_items = []

    order_item_number = 1

    now = datetime.now()

    for order_number in range(
        1,
        number_of_orders + 1
    ):

        # ------------------------------------------
        # Choose an existing customer
        # ------------------------------------------

        customer = random.choice(
            customer_records
        )

        customer_created_at = (
            customer["created_at"].to_pydatetime()
        )


        # ------------------------------------------
        # Generate valid order date
        # ------------------------------------------

        seconds_available = int(
            (
                now
                - customer_created_at
            ).total_seconds()
        )

        order_offset = random.randint(
            0,
            seconds_available
        )

        order_date = (
            customer_created_at
            + timedelta(
                seconds=order_offset
            )
        )


        # ------------------------------------------
        # Order status
        # ------------------------------------------

        status = random.choices(
            [
                "completed",
                "shipped",
                "processing",
                "cancelled"
            ],
            weights=[
                60,
                20,
                15,
                5
            ],
            k=1
        )[0]


        # ------------------------------------------
        # Choose products
        # ------------------------------------------

        number_of_items = random.randint(
            1,
            5
        )

        selected_products = random.sample(
            product_records,
            k=number_of_items
        )


        # ------------------------------------------
        # Create order items
        # ------------------------------------------

        order_total = 0

        order_id = f"O{order_number:07d}"

        for product in selected_products:

            quantity = random.randint(
                1,
                4
            )

            unit_price = float(
                product["price"]
            )

            line_total = (
                quantity
                * unit_price
            )

            order_total += line_total

            order_item = {
                "order_item_id":
                    f"OI{order_item_number:08d}",

                "order_id":
                    order_id,

                "product_id":
                    product["product_id"],

                "quantity":
                    quantity,

                "unit_price":
                    unit_price
            }

            order_items.append(
                order_item
            )

            order_item_number += 1


        # ------------------------------------------
        # Create order
        # ------------------------------------------

        order = {
            "order_id":
                order_id,

            "customer_id":
                customer["customer_id"],

            "order_date":
                order_date,

            "status":
                status,

            "total_amount":
                round(order_total, 2)
        }

        orders.append(order)


    return (
        pd.DataFrame(orders),
        pd.DataFrame(order_items)
    )


# --------------------------------------------------
# Generate
# --------------------------------------------------

orders_df, order_items_df = (
    generate_orders(
        NUMBER_OF_ORDERS
    )
)


# --------------------------------------------------
# Referential integrity validation
# --------------------------------------------------

assert orders_df["order_id"].is_unique

assert order_items_df[
    "order_item_id"
].is_unique


assert set(
    orders_df["customer_id"]
).issubset(
    set(
        customers_df["customer_id"]
    )
)


assert set(
    order_items_df["order_id"]
).issubset(
    set(
        orders_df["order_id"]
    )
)


assert set(
    order_items_df["product_id"]
).issubset(
    set(
        products_df["product_id"]
    )
)


assert (
    order_items_df["quantity"] > 0
).all()


assert (
    order_items_df["unit_price"] > 0
).all()


# --------------------------------------------------
# Validate order totals
# --------------------------------------------------

calculated_totals = (
    order_items_df
    .assign(
        line_total=lambda df:
            df["quantity"]
            * df["unit_price"]
    )
    .groupby("order_id")[
        "line_total"
    ]
    .sum()
    .round(2)
)


stored_totals = (
    orders_df
    .set_index("order_id")[
        "total_amount"
    ]
    .round(2)
)


assert calculated_totals.equals(
    stored_totals
)


# --------------------------------------------------
# Save
# --------------------------------------------------

orders_df.to_csv(
    "data/generated/orders.csv",
    index=False
)

order_items_df.to_csv(
    "data/generated/order_items.csv",
    index=False
)


# --------------------------------------------------
# Output
# --------------------------------------------------

print("Orders:")
print(orders_df.head())

print()

print("Order Items:")
print(order_items_df.head())

print()

print(
    f"Generated {len(orders_df)} orders."
)

print(
    f"Generated {len(order_items_df)} order items."
)

print()

print(
    "Referential integrity validation passed."
)

print(
    "Order total validation passed."
)