# Example: Offset-based Pagination Incremental Sync Strategy
# This connector demonstrates offset-based pagination for incremental syncs.
# It uses an offset and page size to fetch records in batches, saving the offset as state.

# Importing Json for parsing configuration
import json

# Importing requests for fetching data over api calls
import requests as rq

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector, Logging as log, Operations as op


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings required for the connector.
    """
    return [
        {
            "table": "user",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "address": "STRING",
                "company": "STRING",
                "job": "STRING",
                "updatedAt": "UTC_DATETIME",
                "createdAt": "UTC_DATETIME",
            },
        }
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
    log.info("Running offset-based incremental sync")
    base_url = configuration.get("base_url", "http://127.0.0.1:5001/pagination/offset")
    page_size = int(configuration.get("page_size", 50))

    # Get the cursor from state or use default for initial sync
    cursor = state.get("last_updated_at", "0001-01-01T00:00:00Z")

    params = {
        "order_by": "updatedAt",
        "order_type": "asc",
        "updated_since": cursor,
        "limit": page_size,
        "offset": 0,  # Start from offset 0
    }

    sync_items(base_url, params, state)


def sync_items(base_url, params, state):
    """
    Handle the retrieval and processing of paginated API data.
    """
    more_data = True

    while more_data:
        # Get response from API call
        response_page = get_api_response(base_url, params)

        # Process the items
        items = response_page.get("data", [])
        if not items:
            break  # End pagination if there are no records in response

        # Process each user and update state
        for user in items:
            op.upsert(table="user", data=user)
            state["last_updated_at"] = user["updatedAt"]

        # Save progress by checkpointing the state
        op.checkpoint(state)

        # Determine if we should continue pagination
        more_data, params = should_continue_pagination(params, response_page, len(items))


def should_continue_pagination(params, response_page, current_page_size):
    """
    Determine whether pagination should continue based on the API response.
    """
    offset = response_page.get("offset", 0)
    total = response_page.get("total", 0)

    has_more_pages = (offset + current_page_size) < total
    if has_more_pages:
        params["offset"] = offset + current_page_size
    return has_more_pages, params


def get_api_response(base_url, params):
    """
    Send an HTTP GET request to the provided URL with the specified parameters.
    """
    log.info(f"Making API call to url: {base_url} with params: {params}")
    response = rq.get(base_url, params=params)
    response.raise_for_status()
    response_page = response.json()
    return response_page


# required inputs docs https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
