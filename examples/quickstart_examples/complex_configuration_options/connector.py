# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows multiple ways to cast configuration fields to list, integer, boolean and dict for use in connector code.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

import json  # Import the json module to handle JSON data.

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "crypto",  # Name of the table in the destination.
            "primary_key": ["msg"],  # Primary key column(s) for the table.
            # No columns are defined, meaning the types will be inferred.
        }
    ]


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    # Validate required configuration parameters
    required_configs = ["regions", "api_quota", "use_bulk_api", "currencies"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: QuickStart Examples - Complex Configuration Options")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration)

    # converts config string to list of regions
    regions = configuration["regions"].split(",")
    # converts config string to int
    api_quota = int(configuration["api_quota"])
    # converts config string to boolean
    use_bulk_api = configuration["use_bulk_api"].lower() == "true".lower()
    # converts config json string to dict
    parsed_json = json.loads(configuration["currencies"])

    assert isinstance(regions, list) and len(regions) == 3
    assert isinstance(api_quota, int) and api_quota == 12345
    assert isinstance(use_bulk_api, bool) and use_bulk_api
    assert isinstance(parsed_json, list) and len(parsed_json) == 2

    # Upsert operation to insert/update the decrypted message in the "crypto" table.
    op.upsert(table="crypto", data={"msg": "hello world"})

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
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
