# This is a simple example for how to use ConfigurationForm, form_field, and Test
# with the fivetran_connector_sdk module to build a connector with a setup form.
# It shows all available field types (TextField, DropdownField, ToggleField, DescriptiveDropdownField)
# and how to register a setup test that Fivetran runs when the user clicks "Test Connection".
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

import json

import requests

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For building the setup form shown to users when configuring the connector in Fivetran
from fivetran_connector_sdk import ConfigurationForm

# For defining individual form fields (text inputs, dropdowns, toggles, etc.)
from fivetran_connector_sdk import form_field

# For returning success/failure responses from setup test functions
from fivetran_connector_sdk import Test


def configuration_form():
    """
    Define the setup form shown to users when configuring this connector in Fivetran.
    Add fields using add_field() and register connection tests using add_test().
    Fivetran calls this method when rendering the connector setup UI.
    Returns:
        ConfigurationForm: the completed form with fields and tests.
    """
    log.info("Building configuration form")
    config_form = ConfigurationForm()

    # Plain text field — visible input, suitable for non-sensitive values like URLs
    config_form.add_field(
        form_field.TextField(
            name="api_base_url",
            label="API Base URL",
            description="The base URL for your REST API endpoint.",
            required=True,
            placeholder="https://api.example.com/v1",
        )
    )

    # Password field — masked input, suitable for secrets like API keys
    config_form.add_field(
        form_field.TextField(
            name="api_key",
            label="API Key",
            description="Your API authentication key. This value is stored securely.",
            required=True,
            field_type=form_field.TextField.Password,
            placeholder="your_api_key_here",
        )
    )

    # Dropdown field — lets the user select one option from a fixed list
    config_form.add_field(
        form_field.DropdownField(
            name="batch_size",
            label="Batch Size",
            description="Number of records to fetch per API request.",
            values=[10, 100, 500],
        )
    )

    # Toggle field — boolean on/off switch
    config_form.add_field(
        form_field.ToggleField(
            name="enable_metrics",
            label="Enable Metrics",
            description="Log extraction volume metrics (record count and bytes) during each sync.",
        )
    )

    # Descriptive dropdown — like a dropdown but each option includes an explanation
    config_form.add_field(
        form_field.DescriptiveDropdownField(
            name="sync_mode",
            label="Sync Mode",
            values=[
                {
                    "value": "full",
                    "label": "Full Sync",
                    "description": "Re-syncs all records on every run. Use when the source does not expose a modification timestamp.",
                },
                {
                    "value": "incremental",
                    "label": "Incremental Sync",
                    "description": "Syncs only records added or modified since the last run. Requires a cursor field in the API response.",
                },
            ],
        )
    )

    # Register a setup test — Fivetran calls connection_test() when the user clicks "Test Connection"
    config_form.add_test(connection_test, label="Test connection")

    return config_form


def connection_test(configuration: dict):
    """
    Validate that the API credentials are correct and the endpoint is reachable.
    Fivetran calls this function by its __name__ during connector setup.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        Test response indicating success or failure.
    """
    test = Test()
    api_base_url = configuration.get("api_base_url", "").rstrip("/")
    api_key = configuration.get("api_key", "")

    if not api_base_url:
        return test.failure("api_base_url is required.")
    if not api_key:
        return test.failure("api_key is required.")

    try:
        response = requests.get(
            api_base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        if response.status_code == 200:
            log.info("Connection test passed: API returned 200")
            return test.success()
        else:
            log.warning(f"Connection test failed: API returned status {response.status_code}")
            return test.failure(f"API returned status code: {response.status_code}")
    except requests.exceptions.Timeout:
        return test.failure("Connection timed out. Verify your api_base_url is reachable.")
    except requests.exceptions.ConnectionError:
        return test.failure("Could not connect to the API. Check your api_base_url.")
    except requests.exceptions.RequestException as e:
        return test.failure(f"Request failed: {e}")


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
            "table": "post",
            "primary_key": ["id"],
        }
    ]


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    for key in ("api_base_url", "api_key"):
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: '{key}'")


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
              The state dictionary is empty for the first sync or for any full re-sync.
    """
    log.warning("Example: QuickStart Examples - Configuration Form")

    validate_configuration(configuration)

    api_base_url = configuration["api_base_url"].rstrip("/")
    api_key = configuration["api_key"]
    batch_size = int(configuration.get("batch_size", 100))
    enable_metrics = str(configuration.get("enable_metrics", "false")).lower() == "true"
    sync_mode = configuration.get("sync_mode", "full")

    # In incremental mode, resume from where the last sync left off
    cursor = state.get("cursor", 0) if sync_mode == "incremental" else 0

    total_records = 0
    total_bytes = 0

    while True:
        response = requests.get(
            f"{api_base_url}/posts",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"_start": cursor, "_limit": batch_size},
            timeout=30,
        )
        response.raise_for_status()
        posts = response.json()

        if not posts:
            break

        for post in posts:
            op.upsert(table="post", data=post)
            if enable_metrics:
                total_bytes += len(json.dumps(post).encode("utf-8"))

        total_records += len(posts)
        cursor += len(posts)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync
        # process can resume from the correct position in case of the next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint({"cursor": cursor})

        # JSONPlaceholder returns all results when the offset exceeds total; stop when partial page received
        if len(posts) < batch_size:
            break

    if enable_metrics:
        log.info(f"Sync complete: {total_records} records, {total_bytes} bytes extracted")


# This creates the connector object that will use the update, schema, and configuration_form
# functions defined in this connector.py file.
connector = Connector(update=update, schema=schema, configuration_form=configuration_form)

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
# ┌─────┬────────┬───────────────────────┬──────────────────────────────────┐
# │ id  │ userId │         title         │               body               │
# │ int │  int   │        varchar        │             varchar              │
# ├─────┼────────┼───────────────────────┼──────────────────────────────────┤
# │  1  │   1    │ sunt aut facere ...   │ quia et suscipit suscipit ...    │
# │  2  │   1    │ qui est esse          │ est rerum tempore vitae ...      │
# │ ... │  ...   │          ...          │               ...                │
# └─────┴────────┴───────────────────────┴──────────────────────────────────┘
