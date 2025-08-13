from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json
import requests
from datetime import datetime
from utils import flatten_dict


def schema(configuration: dict):
    """Define schema with only primary keys, let Fivetran infer the rest."""
    return [{"table": "events", "primary_key": ["unique_aer_id_number"]}]


def update(configuration: dict, state: dict = None):
    """Main update function that fetches and processes data.

    Args:
        configuration: Dictionary containing connector configuration
        state: Dictionary containing the current state from the last run
    """
    try:
        # Initialize state if not provided
        if state is None:
            state = {}

        # Initialize or update state with default values
        state.setdefault("last_processed", datetime.now().isoformat())
        state.setdefault("total_processed", 0)

        # Base URL for the FDA Veterinary API
        base_url = "https://api.fda.gov/animalandveterinary/event.json"

        # Fetch data with limit of 10 records
        params = {"limit": 10}

        log.info(f"Fetching data from {base_url} with params: {params}")

        # Make the API request
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Process each event
        for event in data.get("results", []):
            # Flatten the nested dictionary
            flattened_event = flatten_dict(event)

            # Upsert the flattened data
            yield op.upsert("events", flattened_event)

            # Update state
            state["total_processed"] += 1

            # Checkpoint after each record
            yield op.checkpoint(state=state)

        log.info(f"Processed {state['total_processed']} records")

    except Exception as e:
        log.severe(f"Error processing data: {str(e)}")
        raise


# Initialize the connector at module level
connector = Connector(update=update, schema=schema)

# For local testing
if __name__ == "__main__":
    import os

    config_path = os.path.join(os.path.dirname(__file__), "configuration.json")
    with open(config_path, "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
