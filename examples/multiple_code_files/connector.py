# This example demonstrates how you can write a complex connector comprising multiple .py files.
# Make sure you place all the .py files in the same directory as connector.py.
# The timestamp_serializer module shown in this example is used to handle scenarios where the source sends timestamps in two different formats.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op

# Import self written modules
from timestamp_serializer import TimestampSerializer


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "event",  # Name of the table in the destination.
            "primary_key": ["name"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "name": "STRING",  # String column for the name.
                "timestamp": "UTC_DATETIME",  # UTC date-time column for the timestamp
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
    timestamp_serializer = TimestampSerializer()

    row_1 = {
        "name": "Event1",
        "timestamp": "2024/09/24 14:30:45"
    }
    row_1['timestamp'] = timestamp_serializer.serialize(row_1['timestamp'])
    yield op.upsert(table="event", data=row_1)

    row_2 = {
        "name": "Event2",
        "timestamp": "2024-09-24 10:30:45"
    }
    row_2['timestamp'] = timestamp_serializer.serialize(row_2['timestamp'])
    yield op.upsert(table="event", data=row_2)


# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition.
connector = Connector(update=update)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()

# Resulting table: TODO : Check why name is not being picked up as PK
# ┌───────────────────┬───────────────────┬────────────────────────┐
# │      name         │    _fivetran_id   │       timestamp        │
# │     varchar       │      varchar      │        varchar         │
# ├───────────────────┼───────────────────┤────────────────────────│
# │       Event1      │     941607216     │  2024-09-24T14:30:45   │
# │       Event2      │     1338900493    │  2024-09-24T10:30:45   │
# └────────────────────────────────────────────────────────────────┘
