from datetime import datetime, timedelta
import random

import pandas as pd
from faker import Faker


fake = Faker()

NUMBER_OF_CUSTOMERS = 1000


def generate_customers(number_of_customers):
    customers = []

    for customer_number in range(1, number_of_customers + 1):

        created_at = fake.date_time_between(
            start_date="-2y",
            end_date="now"
        )

        updated_at = created_at + timedelta(
            days=random.randint(0, 90)
        )

        customer = {
            "customer_id": f"C{customer_number:06d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "country": fake.country(),
            "created_at": created_at,
            "updated_at": updated_at
        }

        customers.append(customer)

    return pd.DataFrame(customers)


customers_df = generate_customers(NUMBER_OF_CUSTOMERS)

customers_df.to_csv(
    "data/generated/customers.csv",
    index=False
)

# print(customers_df.head())
# print()
print(f"Generated {len(customers_df)} customers.")

print()
print("Shape:")
print(customers_df.shape)

print()
print("Missing values:")
print(customers_df.isnull().sum())

print()
print("Duplicate customer IDs:")
print(customers_df["customer_id"].duplicated().sum())

print()
print("Duplicate emails:")
print(customers_df["email"].duplicated().sum())