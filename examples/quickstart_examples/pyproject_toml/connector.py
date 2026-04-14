# This is an example for how to use pyproject.toml for dependency management with the Fivetran Connector SDK.
# Instead of declaring dependencies in a requirements.txt file, this connector uses a pyproject.toml file.
# The pyproject.toml file is the modern Python standard (PEP 621) for declaring project metadata and dependencies.
# The Connector SDK prioritizes pyproject.toml over requirements.txt when both are present.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# This connector fetches the latest exchange rates from the Frankfurter API (https://frankfurter.dev),
# a free, open-source API that requires no authentication.

import json  # Import the json module to handle JSON data.

import requests  # Import the requests module for making HTTP requests. Pre-installed in Fivetran environment.

# Import tenacity for robust API retry logic with exponential backoff.
# This dependency is declared in pyproject.toml under [project.dependencies].
# In production connectors, tenacity is preferred over hand-rolled retry loops because it provides
# configurable retry strategies and cleaner separation of retry logic from business logic.
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# API endpoint for the latest exchange rates.
API_URL = "https://api.frankfurter.dev/v2/rates/latest?base={from_currency}&quotes={to_currency}"
REQUEST_TIMEOUT = 60  # seconds


def log_before_retry(retry_state):
    """Custom before_sleep callback that logs retry attempts using the Fivetran SDK logger."""
    log.warning(
        f"Retry attempt {retry_state.attempt_number} failed: {retry_state.outcome.exception()}. "
        f"Retrying in {retry_state.next_action.sleep:.0f}s..."
    )


# Use tenacity's @retry decorator to handle transient API failures automatically.
# - stop_after_attempt(3): Give up after 3 failed attempts.
# - wait_exponential(multiplier=2, min=2, max=10): Wait 2s, 4s, 8s (capped at 10s) between retries.
# - retry_if_exception_type: Only retry on network/HTTP errors, not on programming errors.
# - before_sleep: Uses a custom callback to log retries via the Fivetran SDK logger (log.warning).
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
    before_sleep=log_before_retry,
    reraise=True,
)
def fetch_exchange_rates(url: str) -> list:
    """Fetch exchange rates from the Frankfurter API with automatic retry on failure."""
    log.info(f"Requesting: {url}")
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "exchange_rates",  # Name of the table in the destination.
            "primary_key": ["date", "from_currency", "to_currency"],  # Composite primary key.
            "columns": {
                "date": "NAIVE_DATE",
                "from_currency": "STRING",
                "to_currency": "STRING",
                "rate": {"type": "DECIMAL", "precision": 15, "scale": 6},
            },
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
    for key in ("from_currency", "to_currency"):
        if key not in configuration or not configuration[key]:
            raise ValueError(f"Missing required configuration value: '{key}'")


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
    log.info("Example: QuickStart Examples - pyproject.toml")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    from_currency = configuration["from_currency"]
    to_currency = configuration["to_currency"]
    log.info(f"Fetching latest exchange rate for {from_currency} -> {to_currency}")

    # Build the API URL for the latest exchange rates.
    url = API_URL.format(from_currency=from_currency, to_currency=to_currency)

    # Fetch data using tenacity-powered retry logic (declared as a dependency in pyproject.toml).
    # tenacity retries up to 3 times on network/HTTP errors with exponential backoff.
    # If all attempts fail, the exception is re-raised and caught below for graceful handling.
    try:
        exchange_rates = fetch_exchange_rates(url)
    except requests.RequestException as e:
        log.severe(f"API request failed after 3 retry attempts: {e}")
        raise RuntimeError(f"Failed to fetch exchange rates from API: {e}") from e

    # The API returns a single record for the configured currency pair.
    if not exchange_rates:
        log.warning("API returned no exchange rate records")
        return

    record = exchange_rates[0]
    log.info(
        f"Exchange rate for {record.get('date')}: {record.get('base')} -> {record.get('quote')} = {record.get('rate')}"
    )

    # Upsert the exchange rate record to the destination table.
    op.upsert(
        table="exchange_rates",
        data={
            "date": record.get("date"),
            "from_currency": record.get("base"),
            "to_currency": record.get("quote"),
            "rate": str(record.get("rate", 0)),
        },
    )

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
# ┌────────────┬───────────────┬─────────────┬────────┐
# │    date    │ from_currency │ to_currency │  rate  │
# │    date    │    varchar    │   varchar   │ decimal│
# ├────────────┼───────────────┼─────────────┼────────┤
# │ 2026-04-14 │ USD           │ INR         │  93.00 │
# └────────────┴───────────────┴─────────────┴────────┘
