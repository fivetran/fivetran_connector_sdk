# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows multiple ways to cast configuration fields to list, integer, boolean and dict for use in connector code.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

import json  # Import the json module to handle JSON data.

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "crypto",  # Name of the table in the destination.
            "primary_key": ["msg"],  # Primary key column(s) for the table.
            # No columns are defined, meaning the types will be inferred.
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    log.warning("Example: QuickStart Examples - Complex Configuration Options")

    # converts config string to list of regions
    regions = configuration['regions'].split(',')
    # converts config string to int
    api_quota = int(configuration['api_quota'])
    # converts config string to boolean
    use_bulk_api = configuration['use_bulk_api'].lower() == 'true'.lower()
    # converts config json string to dict
    parsed_json = json.loads(configuration['currencies'])

    assert isinstance(regions, list) and len(regions) == 3
    assert isinstance(api_quota, int) and api_quota == 12345
    assert isinstance(use_bulk_api, bool) and use_bulk_api
    assert isinstance(parsed_json, list) and len(parsed_json) == 2

    # Yield an upsert operation to insert/update the decrypted message in the "crypto" table.
    yield op.upsert(table="crypto",
                    data={
                        'msg': "hello world"
                    })


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)

# Resulting table:
# ┌────────────────────────────────────────────────┐
# │                      msg                       │
# │                    varchar                     │
# ├────────────────────────────────────────────────┤
# │                   hello world                  │
# └────────────────────────────────────────────────┘
