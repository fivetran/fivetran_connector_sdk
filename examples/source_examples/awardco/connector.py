"""Awardco Connector Example
This connector fetches user data from the AwardCo API and upserts it into the destination using the Fivetran Connector SDK.
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

import requests

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key", "base_url"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "user",
            "primary_key": ["employeeId"],
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

    log.warning("Starting data sync from AwardCo API")

    validate_configuration(configuration=configuration)

    api_key = configuration.get("api_key")
    base_url = configuration.get("base_url")
    last_sync_time = state.get("last_sync_time","1990-01-01T00:00:00")
    new_sync_time = last_sync_time

    try:
        # Use pagination to fetch all users. We request pages until no users are returned
        # or the returned page has fewer items than `per_page`.
        page = 1
        per_page = 100

        while True:
            params = {"page": page, "per_page": per_page}
            response = requests.get(f"{base_url}/api/users", headers={"apiKey": api_key}, params=params)
            response.raise_for_status()
            users = response.json().get("users", [])

            # Stop when there are no users on this page
            if not users:
                break

            for record in users:
                # Upsert each record into the destination table
                op.upsert(table="user", data=record)
                record_time = record.get("updated_at")

                if new_sync_time is None or (record_time and record_time > new_sync_time):
                    new_sync_time = record_time

            # If fewer records than requested were returned, we've reached the last page
            if len(users) < per_page:
                break

            page += 1

        new_state = {"last_sync_time": new_sync_time}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

    except Exception as e:
        raise RuntimeError(f"Failed to sync data: {str(e)}")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
