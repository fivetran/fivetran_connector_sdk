# This example shows how to calculate hash of fields to be used as primary key. This is useful in scenarios where the
# incoming rows do not have any field suitable to be used as a Primary Key.
# Fivetran recommends to define primary keys (https://fivetran.com/docs/connectors/connector-sdk/best-practices#declaringprimarykeys) when writing a connector.
# This is to avoid disruptions caused due to a schema change (addition/removal of a column, etc) that might be needed in the maintainance phase of your connector.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op

# Import built-in Python modules
import hashlib
import json

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
# In this example, there is no use of the configuration
def schema(configuration: dict):
    return [
        {
            "table": "user",  # Name of the table in the destination.
            "primary_key": ["_fivetran_id"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "first_name": "STRING",  # String column for the first name.
                "last_name": "STRING",  # String column for the last name.
                "email": "STRING",  # String column for the email.
                "updated_at": "UTC_DATETIME",  # UTC date-time column for the updated_at.
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
        "first_name": "John",  # First name.
        "last_name": "Doe",  # Last name.
        "email": "john.doe@example.com",  # Email ID.
        "updated_at": "2007-12-03T10:15:30Z"  # Updated at timestamp.
    }

    # Generate hash and add this as a key in the dictionary
    row_1["_fivetran_id"] = generate_row_hash(row_1) # Fivetran recommends to name the hash column as _fivetran_id

    # This source record has None for the email field
    row_2 = {
        "first_name": "Joe",  # First name.
        "last_name": "Smith",  # Last name.
        "email": None,  # Email ID.
        "updated_at": "2014-05-10T00:00:30Z"  # Updated at timestamp.
    }

    row_2["_fivetran_id"] = generate_row_hash(row_2)  # Fivetran recommends to name the hash column as _fivetran_id

    # This source record has the field email missing in the source
    row_3 = {
        "first_name": "Jane",  # First name.
        "last_name": "Dalton",  # Last name.
        "updated_at": None  # Updated at timestamp.
    }

    row_3["_fivetran_id"] = generate_row_hash(row_3)  # Fivetran recommends to name the hash column as _fivetran_id

    # Yield an upsert operation to insert/update the row in the "hello_world" table.
    yield op.upsert(table="user", data=row_1)
    yield op.upsert(table="user", data=row_2)
    yield op.upsert(table="user", data=row_3)


def generate_row_hash(row: dict):
    # Convert dictionary to a sorted JSON string (to ensure consistent ordering)
    row_str = json.dumps(row, sort_keys=True)

    # Create a SHA-1 hash object
    sha1 = hashlib.sha1()

    # Update the hash with the dictionary string encoded in UTF-8
    sha1.update(row_str.encode('utf-8'))

    # Return the hexadecimal representation of the hash
    return sha1.hexdigest()


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()

# Resulting table:
# ┌─────────────────────────────────────────────┬───────────────────┬────────────────────────────────────────────┬───────────────────────────────┐
# │                 _fivetran_id                │   first_name      │     last_name     │       email            │         updated_at            │
# │                     varchar                 │     varchar       │      varchar      │      varchar           |  timestamp with time zone     │
# ├─────────────────────────────────────────────┼───────────────────┼───────────────────┼────────────────────────┤───────────────────────────────│
# │    9507eb591ddb60eb68452d06cf70696d8d5e8140 │       John        │        Doe        │ john.doe@example.com   │ 2007-12-03 10:15:30.000 +0000 │
# │    a211fe507efd31c94e30e91fc8883b4a68605d3d │       Joe         │       Smith       │         [NULL]         │ 2014-05-10 00:00:30.000 +0000 │
# │    aa1f248cbe6e71e1f36db2f09c5b68a2fbe3febb │       Jane        │       Dalton      │         [NULL]         │             [NULL]            │
# └──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘