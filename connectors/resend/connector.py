"""Resend Emails Connector for Fivetran - fetches email data from Resend API.
This connector demonstrates how to fetch email data from Resend REST API and upsert it into destination using the Fivetran Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to Resend API
import requests

# For handling time operations and timestamps
import time

# For type hints
from typing import Optional

__BASE_URL = "https://api.resend.com"  # Base URL for Resend API
__EMAILS_ENDPOINT = "/emails"  # Endpoint path for emails API
__REQUEST_TIMEOUT_SECONDS = 30  # Timeout for API requests in seconds
__CHECKPOINT_INTERVAL = 50  # Checkpoint after every 50 emails

# Retry configuration constants
__MAX_RETRIES = 5  # Maximum number of retry attempts for API requests
__BACKOFF_BASE_SECONDS = 1  # Base delay in seconds for API request retries


def flatten_dict(data: dict, prefix: str = "", separator: str = "_") -> dict:
    """
    Flatten a nested dictionary by concatenating keys with a separator.
    This is used to convert nested JSON responses into flat table structures.

    Args:
        data: The dictionary to flatten
        prefix: Prefix to add to keys (used for recursion)
        separator: Separator to use between nested keys

    Returns:
        A flattened dictionary
    """
    flattened = {}

    for key, value in data.items():
        new_key = f"{prefix}{separator}{key}" if prefix else key

        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            flattened.update(flatten_dict(value, new_key, separator))
        elif isinstance(value, list):
            # Convert lists to JSON strings for storage
            flattened[new_key] = json.dumps(value)
        else:
            flattened[new_key] = value

    return flattened


def fetch_emails_from_api(url: str, headers: dict, params: Optional[dict] = None) -> dict:
    """
    Fetch emails from Resend API with error handling and exponential backoff retry logic.

    Args:
        url: The API endpoint URL
        headers: HTTP headers including authorization
        params: Optional query parameters for pagination

    Returns:
        The JSON response containing email data

    Raises:
        requests.exceptions.RequestException: For HTTP errors
        ValueError: For invalid JSON responses
    """
    params = params or {}

    for attempt in range(__MAX_RETRIES):
        response = None
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=__REQUEST_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            if attempt < __MAX_RETRIES - 1:
                delay_seconds = __BACKOFF_BASE_SECONDS * (2**attempt)
                log.warning(
                    f"Request timeout. Retrying in {delay_seconds} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(delay_seconds)
                continue
            log.severe(f"Request timeout after {__MAX_RETRIES} attempts")
            raise
        except requests.exceptions.HTTPError as e:
            is_retryable_error = response and (
                response.status_code == 429 or response.status_code >= 500
            )
            should_retry = is_retryable_error and attempt < __MAX_RETRIES - 1
            if should_retry:
                delay_seconds = __BACKOFF_BASE_SECONDS * (2**attempt)
                log.warning(
                    f"HTTP {response.status_code} error. Retrying in {delay_seconds} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(delay_seconds)
                continue
            log.severe(f"HTTP error {response.status_code}: {e}")
            raise
        except (requests.exceptions.RequestException, ValueError) as e:
            log.severe(f"Request failed: {e}")
            raise

    raise requests.exceptions.RequestException(f"Request failed after {__MAX_RETRIES} attempts")


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
            "table": "email",  # Table for Resend emails
            "primary_key": ["id"],  # Email ID as primary key
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """

    log.warning("Example: API Connector : Resend Emails Connector")

    # Extract configuration parameters
    api_token = configuration.get("api_token")

    # Set up authentication headers
    headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

    # Get the state variable from previous sync
    last_synced_email_id = state.get("last_synced_email_id")

    try:
        # Sync emails data from /emails endpoint
        new_last_synced_email_id = sync_emails(headers, last_synced_email_id)

        # Update state with the last synced email ID for incremental sync
        new_state = {"last_synced_email_id": new_last_synced_email_id}

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

    except Exception as e:
        log.severe(f"Failed to sync data: {str(e)}")
        raise


def sync_emails(headers: dict, last_synced_email_id: Optional[str] = None) -> Optional[str]:
    """
    Fetch and sync emails data from Resend API with incremental syncing support.
    This function handles pagination and checkpoints progress at regular intervals.

    Args:
        headers: HTTP headers including authorization token
        last_synced_email_id: Email ID from last sync for incremental updates

    Returns:
        The last synced email ID for the next sync
    """
    log.info("Starting emails sync")

    # Build the URL for emails endpoint
    url = f"{__BASE_URL}{__EMAILS_ENDPOINT}"
    params: dict = {}

    emails_synced_count = 0
    last_email_id = last_synced_email_id
    has_more_pages = True

    # Process emails in pages until no more data available
    while has_more_pages:
        try:
            response_data = fetch_emails_from_api(url, headers, params)
            emails = response_data.get("data", [])

            log.info(f"Retrieved {len(emails)} emails from API")

            # Process each email in the current page
            for email in emails:
                # Flatten the email data for table storage
                flattened_email = flatten_dict(email)

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="email", data=flattened_email)

                emails_synced_count += 1
                last_email_id = email.get("id")

                # Checkpoint at regular intervals to save progress
                if emails_synced_count % __CHECKPOINT_INTERVAL == 0:
                    checkpoint_state = {"last_synced_email_id": last_email_id}

                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)
                    log.info(f"Checkpointed at {emails_synced_count} emails")

            # Check pagination - Resend uses has_more flag
            has_more_pages = response_data.get("has_more", False)

            if not has_more_pages:
                log.info(f"No more emails to sync. Total synced: {emails_synced_count}")
                break

            # For pagination, use after parameter with the last email ID
            if emails:
                params["after"] = last_email_id
                log.info(f"Fetching next page after email ID: {last_email_id}")
            else:
                break

        except (requests.exceptions.RequestException, ValueError) as e:
            log.severe(f"Error syncing emails: {e}")
            raise

    log.info(f"Email sync completed. Total emails synced: {emails_synced_count}")
    return last_email_id


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Test the connector locally
    connector.debug()
