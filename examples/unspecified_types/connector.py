# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows you the inference that is applied if you send data without defining its type in the Schema() method.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

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
            "table": "unspecified",  # Name of the table in the destination.
            "primary_key": ["id"],  # Primary key column(s) for the table.
            # Note: No columns are defined, meaning the types will be inferred.
        },
    ]


# Define the update function, which is a required function, and will be used to perform operations in the connector.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    # Yield an upsert operation to insert/update the row in the "unspecified" table.
    # The data dictionary contains various data types.
    # Since the schema does not specify column types, they will be inferred from the data.
    yield op.upsert(
        table="unspecified",
        data={
            "id": 1,  # Primary key.
            "_bool": True,  # Boolean value.
            "_short": 15,  # Short integer value.
            "_long": 132353453453635,  # Long integer value.
            "_dec": "105.34",  # Decimal value as a string.
            "_float": 10.4,  # Floating-point value.
            "_double": 1e-4,  # Double precision floating-point value.
            "_ndate": "2007-12-03",  # Naive date value.
            "_ndatetime": "2007-12-03T10:15:30",  # Naive date-time value.
            "_utc": "2007-12-03T10:15:30.123Z",  # UTC date-time value.
            "_binary": b"\x00\x01\x02\x03",  # Binary data.
            "_xml": "<tag>This is XML</tag>",  # XML data.
            "_str": "This is a string",  # String data.
            "_json": {"a": 10},  # JSON data.
            "_null": None  # Null value.
        }
    )


# Instantiate a Connector object from the Connector class, passing the update and schema functions as parameters.
# This creates a new connector that will use these functions to define its behavior.
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    result = connector.debug()
