# This is a simple example for how to work with next-page-url pagination for a REST API.
# It defines a simple `update` method, which upserts retrieved data to a table named "item".
# THIS EXAMPLE IS THE JUST FOR UNDERSTANDING AND NEEDS MODIFICATION OF A REAL API URL TO MAKE IT WORK.
# See the Technical Reference documentation
# (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import requests to make HTTP calls to API
import requests as rq
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
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


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    base_url = "http://127.0.0.1:5052/pagination/next_page_url"

    # Retrieve the cursor from the state to determine the current position in the data sync.
    # If the cursor is not present in the state, start from the beginning of time ('0001-01-01T00:00:00Z').
    cursor = state['last_updated_at'] if 'last_updated_at' in state else '0001-01-01T00:00:00Z'

    params = {
        "order_by": "updatedAt",
        "order": "asc",
        "updated_since": cursor,
        "per_page": 10,
    }

    current_url = base_url  # Start with the base URL and initial params

    yield from sync_items(current_url, params, state)


# The sync_items function handles the retrieval and processing of paginated API data.
# It performs the following tasks:
# 1. Sends an API request to the specified URL with the provided parameters.
# 2. Processes the items returned in the API response by yielding upsert operations to Fivetran.
# 3. Updates the state with the 'updatedAt' timestamp of the last processed item.
# 4. Saves the state periodically to ensure the sync can resume from the correct point.
# 5. Continues fetching and processing data from the API until no more pages are available.
#
# The function takes four parameters:
# - current_url: The URL to the API endpoint, which may be updated with the next page's URL during pagination.
# - params: A dictionary of query parameters to be sent with the API request.
# - state: A dictionary representing the current state of the sync, including the last 'updatedAt' timestamp.
def sync_items(current_url, params, state):
    more_data = True

    while more_data:
        # Get response from API call.
        response_page = get_api_response(current_url, params)

        # Process the items
        items = response_page.get("data", [])
        if not items:
            more_data = False  # End pagination if there are no records in response

        # Iterate over each item in the 'items' list and yield an upsert operation.
        # The 'upsert' operation inserts the data into the destination.
        # Update the state with the 'updatedAt' timestamp of the current item.
        for user in items:
            yield op.upsert(table="user", data=user)
            state["last_updated_at"] = user["updatedAt"]

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.
        yield op.checkpoint(state)

        current_url, more_data, params = should_continue_pagination(current_url, params, response_page)


def should_continue_pagination(current_url, params, response_page):
    has_more_pages = True

    next_page_url = get_next_page_url_from_response(response_page)

    if next_page_url:
        current_url = next_page_url
        params = {}  # Clear params since the next URL contains the query params
    else:
        has_more_pages = False  # End pagination if there is no 'next' URL in the response.

    return current_url, has_more_pages, params


def get_api_response(current_url, params):
    log.info(f"Making API call to url: {current_url} with params: {params}")
    response = rq.get(current_url, params=params)
    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
    response_page = response.json()
    return response_page


def get_next_page_url_from_response(response_page):
    return response_page.get("next_page_url")


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module. This is Python's standard entry method allowing your script to
# be run directly from the command line or IDE 'run' button. This is useful for debugging while you write your code.
# Note this method is not called by Fivetran when executing your connector in production. Please test using the
# Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from
    # your IDE.
    connector.debug()

# Resulting table:
# ┌───────┬───────────────┬─────────────────────────────┬──────────────────────────┬───────────────────────────┐
# │  id   │      name     │          description        │        updatedAt         │        createdAt          │
# │ string│      string   │           string            │      timestamp with UTC  │      timestamp with UTC   │
# ├───────┼───────────────┼─────────────────────────────┼──────────────────────────┼───────────────────────────┤
# │ 001   │ Widget A      │ A basic widget              │ 2024-08-12T15:30:00Z     │ 2024-08-01T10:00:00Z      │
# │ 002   │ Widget B      │ A more advanced widget      │ 2024-08-11T14:45:00Z     │ 2024-08-02T11:15:00Z      │
# │ 003   │ Widget C      │ The ultimate widget         │ 2024-08-10T09:20:00Z     │ 2024-08-03T12:30:00Z      │
# │ 004   │ Widget D      │ A special edition widget    │ 2024-08-09T16:05:00Z     │ 2024-08-04T13:40:00Z      │
# │ 005   │ Widget E      │ An eco-friendly widget      │ 2024-08-08T08:10:00Z     │ 2024-08-05T14:25:00Z      │
# ├───────┴───────────────┴─────────────────────────────┴──────────────────────────┴───────────────────────────┤
# │  5 rows                                                                                          5 columns │
# └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
