# This is an example for how to work with the fivetran_connector_sdk module.
"""Add one line description of your connector here.
For example: This connector demonstrates how to fetch data from XYZ source and upsert it into destination using ABC library.
"""
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details


# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op


""" Add your source-specific imports here
Example: import pandas, boto3, etc.
Add comment for each import to explain its purpose for users to follow."""
import json


"""
GUIDELINES TO FOLLOW WHILE WRITING AN EXAMPLE CONNECTOR:
- Import only the necessary modules and libraries to keep the code clean and efficient.
- Use clear, consistent and descriptive names for your functions and variables.
- For constants and global variables, use uppercase letters with underscores (e.g. CHECKPOINT_INTERVAL, TABLE_NAME).
- Add comments to explain the purpose of each function in the docstring.
- Add comments to explain the purpose of complex logic within functions, where necessary.
- Add comments to highlight where users can make changes to the code to suit their specific use case.
- Split your code into smaller functions to improve readability and maintainability where required.
- Use logging to provide useful information about the connector's execution. Do not log excessively.
- Implement error handling to catch exceptions and log them appropriately. Catch specific exceptions where possible.
- Define the complete data model with primary key and data types in the schema function.
- Ensure that the connector does not load all data into memory at once. This can cause memory overflow errors. Use pagination or streaming where possible.
- Add comments to explain pagination or streaming logic to help users understand how to handle large datasets.
- Add comments for upsert, update and delete to explain the purpose of upsert, update and delete. This will help users understand the upsert, update and delete processes.
- Checkpoint your state at regular intervals to ensure that the connector can resume from the last successful sync in case of interruptions.
- Add comments for checkpointing to explain the purpose of checkpoint. This will help users understand the checkpointing process.
- Refer to the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices)
"""


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
    required_configs = ["param1", "param2", "param3"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


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
            "table": "table_name",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
            "columns": {  # Definition of columns and their types, optional.
                "id": "STRING",  # Contains a dictionary of column names and data types
            },  # For any columns whose names are not provided here, e.g. id, their data types will be inferred
        },
    ]


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

    log.warning("Example: <type_of_example> : <name_of_the_example>")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Extract configuration parameters as required
    param1 = configuration.get("param1")

    # Get the state variable for the sync, if needed
    last_sync_time = state.get("last_sync_time")

    try:
        data = get_data()
        for record in data:

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="table_name", data=record)

        # Update state with the current sync time for the next run
        new_state = {"last_sync_time": new_sync_time}

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
