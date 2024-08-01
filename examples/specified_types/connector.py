# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows how to use the Schema() method to define columns for all supported data types.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op


# Define the schema function which lets you configure the schema your connector delivers.
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "specified",  # Name of the table in the destination.
            "primary_key": ["_bool"],  # Primary key column(s) for the table.
            "columns": {
                "_bool": "BOOLEAN",  # Boolean data type.
                "_short": "SHORT",  # Short integer data type.
                "_long": "LONG",  # Long integer data type.
                "_dec": {  # Decimal data type with precision and scale.
                    "type": "DECIMAL",
                    "precision": 15,
                    "scale": 2
                },
                "_float": "FLOAT",  # Floating-point data type.
                "_double": "DOUBLE",  # Double precision floating-point data type.
                "_ndate": "NAIVE_DATE",  # Naive date data type.
                "_ndatetime": "NAIVE_DATETIME",  # Naive date-time data type.
                "_utc": "UTC_DATETIME",  # UTC date-time data type.
                "_binary": "BINARY",  # Binary data type.
                "_xml": "XML",  # XML data type.
                "_str": "STRING",  # String data type.
                "_json": "JSON",  # JSON data type.
                "_null": "STRING"  # String data type, can handle null values.
            }
        }
    ]


# Define the update function, which is a required function, and will be used to perform operations in the connector.
# The update function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    # Yield an upsert operation to insert/update the row in the "specified" table.
    yield op.upsert(
        table="specified",
        data={
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
