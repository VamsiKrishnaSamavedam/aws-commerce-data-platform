from datetime import datetime, timedelta
import random

import pandas as pd
from faker import Faker


# --------------------------------------------------
# Configuration
# --------------------------------------------------

NUMBER_OF_CUSTOMERS = 1000
RANDOM_SEED = 42

random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)

fake = Faker()


# --------------------------------------------------
# Customer generator
# --------------------------------------------------

def generate_customers(number_of_customers):
    customers = []

    now = datetime.now()

    for customer_number in range(1, number_of_customers + 1):

        created_at = fake.date_time_between(
            start_date="-2y",
            end_date=now
        )

        # Maximum allowed update time:
        # either 90 days after creation or the current time,
        # whichever comes first.

        seconds_since_creation = int(
            (now - created_at).total_seconds()
        )

        max_update_seconds = min(
            seconds_since_creation,
            90 * 24 * 60 * 60
        )

        update_offset = random.randint(
            0,
            max_update_seconds
        )

        updated_at = created_at + timedelta(
            seconds=update_offset
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


# --------------------------------------------------
# Generate data
# --------------------------------------------------

customers_df = generate_customers(
    NUMBER_OF_CUSTOMERS
)


# --------------------------------------------------
# Basic validation
# --------------------------------------------------

assert customers_df["customer_id"].is_unique
assert customers_df["email"].is_unique

assert (
    customers_df["updated_at"]
    >= customers_df["created_at"]
).all()

assert (
    customers_df["updated_at"]
    <= datetime.now()
).all()


# --------------------------------------------------
# Save
# --------------------------------------------------

customers_df.to_csv(
    "data/generated/customers.csv",
    index=False
)


# --------------------------------------------------
# Output
# --------------------------------------------------

print(customers_df.head())

print()
print(f"Generated {len(customers_df)} customers.")

print()
print("Customer validation passed.")