# This is a simple example for how to work with the fivetran_connector_sdk module.
# It defines a simple `update` method, which upserts some data to a table named "hello".
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op


# Define the update function, which is a required function, which will be used to perform operations in the connector.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    # The yield statement returns a generator object.
    # This generator will yield an upsert operation to the Fivetran connector.
    # The op.upsert method is called with two arguments:
    # - The first argument is the name of the table to upsert the data into, in this case, "hello".
    # - The second argument is a dictionary containing the data to be upserted,
    yield op.upsert("hello", {"message": "hello, world!"})


# Instantiate a Connector object from the Connector class, passing the update function as a parameter.
# This creates a new connector that will use the update function to run updates.
connector = Connector(update=update)

if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()
