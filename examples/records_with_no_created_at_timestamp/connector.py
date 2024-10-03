# This is an example to show how to work with records where the source does not provide a created_at(or equivalent) field.
# If you want to keep track of when the record was first observed, then the updated_at(or equivalent, eg, modified_at etc)
# field can be made part of the composite Primary Key. By doing this you can ensure that successive syncs do not rewrite the
# updated_at field in the existing row in the destination. Instead, it creates a new row with the new updated_at.
# Now, the record with earliest value of _fivetran_synced system column represents when the record was first observed in source.
# Refer to the Resulting table section at the bottom of this file for a detailed demonstration.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op


def schema(configuration: dict):
    return [
        {
            "table": "user",  # Name of the table in the destination.
            "primary_key": ["id", "updated_at"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "id": "INT",
                "updated_at": "UTC_DATETIME",  # UTC date-time column for the updated_at.
                "first_name": "STRING",  # String column for the first name.
                "last_name": "STRING",  # String column for the last name.
                "designation": "STRING"
            },
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    # Represents a record fetched from source
    row_1 = {
        "id": 123,
        "first_name": "John",  # First name.
        "last_name": "Doe",  # Last name.
        "designation": "Manager",  # Designation
        "updated_at": "2007-12-03T10:15:30Z"  # Updated at timestamp.
    }

    # Now lets say the record represented by row_1 gets updated in source as below:
    row_1_updated = {
        "id": 123,
        "first_name": "John",
        "last_name": "Doe",
        "designation": "Senior Manager",  # Value changed
        "updated_at": "2008-01-04T23:44:21Z"  # Updated at changed
    }

    # Represents another record fetched from source
    row_2 = {
        "id": 456,
        "first_name": "Jane",  # First name.
        "last_name": "Dalton",  # Last name.
        "designation": "VP",  # Designation
        "updated_at": "2008-11-12T00:00:20Z"  # Updated at timestamp.
    }

    yield op.upsert(table="user", data=row_1)
    yield op.upsert(table="user", data=row_1_updated)
    yield op.upsert(table="user", data=row_2)


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()

# Resulting table:
# ┌───────────┬────────────────────────────┬───────────────────┬───────────────────┬────────────────────────┐
# │    id     │         updated_at         │   first_name      │     last_name     │      designation       │
# │  integer  │  timestamp with time zone  │     varchar       │      varchar      │        varchar         │
# ├───────────┼────────────────────────────│───────────────────┼───────────────────┤────────────────────────│
# │    123    │    2007-12-03T10:15:30Z    │       John        │        Doe        │        Manager         │
# │    123    │    2008-01-04T23:44:21Z    │       John        │        Doe        │     Senior Manager     │
# │    456    │    2008-11-12T00:00:20Z    │       Jane        │       Dalton      │          VP            │
# └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
# Each sync will capture the latest update and will sync it as an additional record in the destination.
# Multiple updates between syncs cannot be captured this way. For explanation see https://fivetran.com/docs/core-concepts/sync-modes/history-mode#changestodatabetweensyncs
