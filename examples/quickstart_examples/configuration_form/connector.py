# This is an example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to define a custom ConfigurationForm for your connector.
# The ConfigurationForm defines the fields shown in the Fivetran dashboard setup wizard.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import protobuf classes to build ConfigurationFormResponse
from fivetran_connector_sdk.protos.common_pb2 import (
    ConfigurationFormResponse,
    DropdownField,
    FormField,
    TextField,
    ToggleField,
)


def configuration_form(configuration: dict):
    """
    Define the configuration_form function which lets you configure the setup form your connector shows
    in the Fivetran dashboard.
    See the technical reference documentation for more details on the configuration_form function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#configurationform
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return ConfigurationFormResponse(
        schema_selection_supported=False,
        table_selection_supported=False,
        fields=[
            # Plain text field
            FormField(
                name="username",
                label="Username",
                description="Your account username",
                required=True,
                text_field=TextField.PlainText,
            ),
            # Password field - masked in the UI
            FormField(
                name="password",
                label="Password",
                description="Your account password",
                required=True,
                text_field=TextField.Password,
            ),
            # Dropdown field
            FormField(
                name="region",
                label="Region",
                description="The region to connect to",
                required=False,
                dropdown_field=DropdownField(dropdown_field=["US", "EU", "AP"]),
                default_value="US",
            ),
            # Toggle field
            FormField(
                name="enable_debug",
                label="Enable Debug Logs",
                description="Turn on verbose debug logging",
                toggle_field=ToggleField(),
            ),
        ],
    )


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
    log.warning("Example: QuickStart Examples - Configuration Form")

    username = configuration.get("username", "")
    region = configuration.get("region", "US")
    log.info(f"Syncing data for user: {username}, region: {region}")

    op.upsert(table="hello", data={"message": "hello, world!", "username": username, "region": region})

    op.checkpoint(state)


# This creates the connector object that will use the update and configuration_form functions defined in this connector.py file.
connector = Connector(update=update, configuration_form=configuration_form)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    connector.debug()
