# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows the three operations you can use to deliver data to Fivetran.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

import uuid  # Import the uuid module to generate unique identifiers.

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "three",  # Name of the table in the destination.
            "primary_key": ["id"],  # Primary key column(s) for the table.
        }
    ]


# Define the update function, which is a required function, and will be used to perform operations in the connector.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    # Generate three unique identifiers using the uuid4 method.
    ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]

    # Loop through the generated ids and yield an upsert operation for each.
    for ii, id in enumerate(ids):
        print(f"adding {id}")
        # Yield an upsert operation to insert/update the row in the "three" table.
        yield op.upsert(table="three", data={"id": id, "val1": id, "val2": ii})

    print(f"updating {ids[1]} to 'abc'")
    # Yield an update operation to modify the row with the second id in the "three" table.
    yield op.update(table="three", modified={"id": ids[1], "val1": "abc"})

    print(f"deleting {ids[2]}")
    # Yield a delete operation to remove the row with the third id from the "three" table.
    yield op.delete(table="three", keys={"id": ids[2]})


# Instantiate a Connector object from the Connector class, passing the update and schema functions as parameters.
# This creates a new connector that will use these functions to define its behavior.
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()
