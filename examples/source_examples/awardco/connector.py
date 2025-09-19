"""This connector fetches user data from the AwardCo API and upserts it into the destination using the Fivetran Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

import json
from pathlib import Path
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
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
            "columns": {
                "employeeId": "STRING",
                "firstName": "STRING",
                "lastName": "STRING",
                "email": "STRING",
                "balance": "FLOAT",
                "currencyCode": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
    """
    log.warning("Starting data sync from AwardCo API")

    validate_configuration(configuration=configuration)

    api_key = configuration.get("api_key")
    base_url = configuration.get("base_url")
    use_mock = str(configuration.get("use_mock", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }
    last_sync_time = state.get("last_sync_time","1990-01-01T00:00:00")
    new_sync_time = last_sync_time

    try:
        if use_mock:
            log.info("Using local mock data from files/mock_users.json")
            mock_path = Path(__file__).parent / "files" / "mock_users.json"
            with mock_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            data = payload.get("users", [])
        else:
            response = requests.get(f"{base_url}/api/users", headers={"apiKey": api_key})
            response.raise_for_status()
            data = response.json().get("users", [])

        for record in data:
            op.upsert(table="user", data=record)
            record_time = record.get("updated_at")

            if new_sync_time is None or (record_time and record_time > new_sync_time):
                new_sync_time = record_time

        new_state = {"last_sync_time": new_sync_time}
        op.checkpoint(new_state)

    except Exception as e:
        raise RuntimeError(f"Failed to sync data: {str(e)}")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
