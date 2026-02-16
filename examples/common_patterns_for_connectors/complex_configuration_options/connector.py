"""
This is an example to show how to handle complex configuration options in your connector code.
It shows multiple ways to cast configuration fields to list, integer, boolean and dict for use in connector code.
It also shows how to define constant values in a separate file and import them into your connector.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# Import the json module to handle JSON data.
import json

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import constant from a separate file.
from constant import API_CONSTANT


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


def parse_and_get_values(configuration):
    """
    This function is responsible for fetching and parsing the configuration values.
    This function checks if the values are present in Fivetran connector configuration
    If they are not present, it uses the constant defined in constant.py
    You can modify the logic in this function to suit your needs.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A tuple containing the values: regions, api_quota, use_bulk_api, currencies, complex_constant
    """
    if "complex_constant" in configuration:
        log.info("using Fivetran connector configuration")
        # Validate the configuration to ensure it contains all required values.
        validate_configuration(configuration)

        # converts config string to list of regions
        regions = configuration["regions"].split(",")
        # converts config string to int
        api_quota = int(configuration["api_quota"])
        # converts config string to boolean
        use_bulk_api = configuration["use_bulk_api"].lower() == "true".lower()
        # converts config json string to dict
        currencies = json.loads(configuration["currencies"])
        # converts complex constant json string to dict
        complex_constant = json.loads(configuration["complex_constant"])

        return regions, api_quota, use_bulk_api, currencies, complex_constant

    else:
        # If the values are not present, use the constant defined in constant.py
        # Do not store secrets here.
        # Always use Fivetran connector configuration ( configuration.json ) for sensitive values such as API keys or credentials.
        log.warning("using constant defined in constant.py")
        return (
            API_CONSTANT.get("regions"),
            API_CONSTANT.get("api_quota"),
            API_CONSTANT.get("use_bulk_api"),
            API_CONSTANT.get("currencies"),
            API_CONSTANT.get("complex_constant"),
        )


def validate_fetched_values(regions, api_quota, use_bulk_api, currencies, complex_constant):
    """
    This is a test function to ensure that the values fetched from parse_and_get_values() function are of the expected types and formats.
    You will not typically need to define a function like this in your connector code
    But it is included here for demonstration purposes to validate and test the values you fetch.
    Args:
        regions: regions value, expected to be a list of strings
        api_quota: api_quota value, expected to be an integer
        use_bulk_api: use_bulk_api value, expected to be a boolean
        currencies: currencies value, expected to be a list of dictionaries with 'From' and 'To' keys
        complex_constant: complex_constant value, expected to be a nested dictionary with specific structure
    """
    assert isinstance(regions, list) and len(regions) == 3
    assert isinstance(api_quota, int) and api_quota == 12345
    assert isinstance(use_bulk_api, bool) and use_bulk_api
    assert isinstance(currencies, list) and len(currencies) == 2
    assert isinstance(complex_constant, dict)


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
    log.warning("Example: Common patterns for connectors - Complex Configuration Options")

    # Get the values
    regions, api_quota, use_bulk_api, currencies, complex_constant = parse_and_get_values(
        configuration
    )

    # Validate the fetched values to ensure they are of the expected types and formats.
    validate_fetched_values(regions, api_quota, use_bulk_api, currencies, complex_constant)

    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(table="crypto", data={"msg": "hello world"})

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)

# Resulting table:
# ┌────────────────────────────────────────────────┐
# │                      msg                       │
# │                    varchar                     │
# ├────────────────────────────────────────────────┤
# │                   hello world                  │
# └────────────────────────────────────────────────┘
