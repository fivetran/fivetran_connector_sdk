# This module simulates an external API that returns three different nested data shapes.
# Each function corresponds to one of the three data handling patterns demonstrated in connector.py:
#
#   get_users_with_address()  → Pattern 1: single nested object  (flatten into columns)
#   get_users_with_orders()   → Pattern 2: nested list           (break into child tables)
#   get_users_with_metadata() → Pattern 3: complex nested object (write as JSON blob)
#
# In a real-world connector, replace each function body with your actual API call or
# database query logic. Add pagination, retry handling, and rate-limit backoff as needed.

# Import Faker to generate realistic synthetic data for local testing and debugging.
from faker import Faker

# For enabling logs in your connector code.
from fivetran_connector_sdk import Logging as log

fake = Faker()


def get_users_with_address():
    """
    Simulate fetching users that each contain a single nested 'address' object.
    Used to demonstrate Pattern 1: flattening a nested object into individual columns.

    In a real-world connector, replace this with your actual API call or database query.
    Add pagination if your source returns results across multiple pages.
    Add retry logic with exponential backoff to handle transient API errors.

    Returns:
        A list of dictionaries, each representing a user with a nested address object.
        Example record shape:
          {
            "id": "...",
            "name": "Alice",
            "address": {"city": "New York", "zip": "10001"}
          }
    """
    log.info("Making API call: fetching users with nested address object")
    users = []
    for _ in range(fake.random_int(min=1, max=6)):
        users.append(
            {
                "id": fake.uuid4(),
                "name": fake.name(),
                # Nested address object — will be flattened into columns in connector.py.
                "address": {
                    "city": fake.city(),
                    "zip": fake.zipcode(),
                },
            }
        )
    return users


def get_users_with_orders():
    """
    Simulate fetching users that each contain a nested list of 'orders'.
    Used to demonstrate Pattern 2: breaking a nested list into parent and child tables.

    In a real-world connector, replace this with your actual API call or database query.
    Add pagination if your source returns results across multiple pages.
    Add retry logic with exponential backoff to handle transient API errors.

    Returns:
        A list of dictionaries, each representing a user with a nested orders list.
        Example record shape:
          {
            "user_id": "...",
            "name": "Alice",
            "orders": [
              {"order_id": "A1", "amount": 20},
              {"order_id": "B2", "amount": 35}
            ]
          }
    """
    log.info("Making API call: fetching users with nested orders list")
    users = []
    for _ in range(fake.random_int(min=1, max=6)):
        user_id = fake.uuid4()
        orders = [
            {
                "order_id": fake.uuid4(),
                "amount": round(fake.pyfloat(min_value=10, max_value=500, right_digits=2), 2),
            }
            # Each user has between 1 and 4 orders.
            for _ in range(fake.random_int(min=1, max=4))
        ]
        users.append(
            {
                "user_id": user_id,
                "name": fake.name(),
                # Nested orders list — will be split into a child table in connector.py.
                "orders": orders,
            }
        )
    return users


def get_users_with_metadata():
    """
    Simulate fetching users that each contain a complex, deeply nested 'metadata' object.
    Used to demonstrate Pattern 3: storing a variable-structure object as a JSON blob column.

    In a real-world connector, replace this with your actual API call or database query.
    Add pagination if your source returns results across multiple pages.
    Add retry logic with exponential backoff to handle transient API errors.

    Returns:
        A list of dictionaries, each representing a user with a complex metadata object.
        Example record shape:
          {
            "id": "...",
            "name": "Alice",
            "metadata": {
              "preferences": {"theme": "dark", "notifications": true},
              "history": ["event_1", "event_2"]
            }
          }
    """
    log.info("Making API call: fetching users with complex metadata object")
    users = []
    for _ in range(fake.random_int(min=1, max=6)):
        users.append(
            {
                "id": fake.uuid4(),
                "name": fake.name(),
                # Deeply nested metadata object — will be stored as a JSON blob in connector.py.
                "metadata": {
                    "preferences": {
                        "theme": fake.random_element(["dark", "light"]),
                        "notifications": fake.boolean(),
                    },
                    "history": [fake.sentence() for _ in range(fake.random_int(min=1, max=3))],
                },
            }
        )
    return users
