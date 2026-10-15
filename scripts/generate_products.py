import random

import pandas as pd


# --------------------------------------------------
# Configuration
# --------------------------------------------------

RANDOM_SEED = 42

random.seed(RANDOM_SEED)


# --------------------------------------------------
# Product catalog
# --------------------------------------------------

PRODUCT_CATALOG = {
    "Electronics": [
        "Wireless Mouse",
        "Mechanical Keyboard",
        "USB-C Hub",
        "Bluetooth Speaker",
        "Webcam",
        "Laptop Stand",
        "Wireless Headphones",
        "Portable Charger"
    ],

    "Home": [
        "Coffee Maker",
        "Desk Lamp",
        "Storage Basket",
        "Electric Kettle",
        "Throw Pillow",
        "Wall Clock",
        "Water Bottle",
        "Kitchen Scale"
    ],

    "Sports": [
        "Yoga Mat",
        "Resistance Bands",
        "Running Belt",
        "Foam Roller",
        "Jump Rope",
        "Workout Gloves",
        "Gym Bag",
        "Training Bottle"
    ],

    "Books": [
        "Python Fundamentals",
        "SQL Fundamentals",
        "Cloud Computing Guide",
        "Data Engineering Handbook",
        "Machine Learning Basics",
        "System Design Guide",
        "Statistics Essentials",
        "Algorithms Handbook"
    ],

    "Clothing": [
        "Classic T-Shirt",
        "Running Shorts",
        "Hoodie",
        "Sports Jacket",
        "Casual Shirt",
        "Sweatpants",
        "Baseball Cap",
        "Training Socks"
    ]
}


PRICE_RANGES = {
    "Electronics": (20, 250),
    "Home": (10, 150),
    "Sports": (10, 120),
    "Books": (15, 80),
    "Clothing": (10, 150)
}


# --------------------------------------------------
# Product generator
# --------------------------------------------------

def generate_products():
    products = []

    product_number = 1

    for category, product_names in PRODUCT_CATALOG.items():

        minimum_price, maximum_price = PRICE_RANGES[
            category
        ]

        for product_name in product_names:

            product = {
                "product_id": f"P{product_number:05d}",
                "product_name": product_name,
                "category": category,
                "price": round(
                    random.uniform(
                        minimum_price,
                        maximum_price
                    ),
                    2
                )
            }

            products.append(product)

            product_number += 1

    return pd.DataFrame(products)


# --------------------------------------------------
# Generate
# --------------------------------------------------

products_df = generate_products()


# --------------------------------------------------
# Validation
# --------------------------------------------------

assert products_df["product_id"].is_unique

assert (
    products_df["price"] > 0
).all()


# --------------------------------------------------
# Save
# --------------------------------------------------

products_df.to_csv(
    "data/generated/products.csv",
    index=False
)


# --------------------------------------------------
# Output
# --------------------------------------------------

print(products_df.head(10))

print()
print(f"Generated {len(products_df)} products.")

print()
print("Product validation passed.")