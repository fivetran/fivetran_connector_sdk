# This is a simple example for how to work with next_page_url pagination for a REST API.
# It defines a simple `update` method, which upserts retrieved data to a table named "item".
# This example is the just for understanding and needs modification of a real API url to make it work.
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
            "table": "item",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "description": "STRING",
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
    base_url = "https://example.com/api/items/"  # TODO: REPLACE WITH ACTUAL URL BEFORE RUNNING

    # Retrieve the cursor from the state to determine the current position in the data sync.
    # If the cursor is not present in the state, start from the beginning of time ('0001-01-01T00:00:00Z').
    cursor = state['last_updated_at'] if 'last_updated_at' in state else '0001-01-01T00:00:00Z'

    params = {
        "order_by": "updatedAt",
        "order": "asc",
        "since": cursor,
        "per_page": 100,
    }

    more_data = True
    current_url = base_url  # Start with the base URL and initial params

    yield from sync_items(current_url, more_data, params, state)


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
# - more_data: A boolean flag indicating whether more data is available for sync.
# - params: A dictionary of query parameters to be sent with the API request.
# - state: A dictionary representing the current state of the sync, including the last 'updatedAt' timestamp.
#
# API response will look like the following structure:
# {
#   "pages": {
#     // pagination-data
#   },
#   "data": [
#     {
#        // item-record
#     },
#     ...
#   ]
# }
def sync_items(current_url, more_data, params, state):
    while more_data:
        # Get response from API call.
        response_page = get_api_response(current_url, params)

        # Process the items
        items = response_page.get("items", [])
        if not items:
            more_data = False  # End pagination if there are no records in response

        # Iterate over each item in the 'items' list and yield an upsert operation.
        # The 'upsert' operation inserts the data into the destination.
        # Update the state with the 'updatedAt' timestamp of the current item.
        for item in items:
            yield op.upsert(table="item", data=item)
            state["last_updated_at"] = item["updatedAt"]

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.
        op.checkpoint(state)

        # Check if there is a next page URL in the response to continue the pagination
        next_page_url = get_next_page_url_from_response(response_page)
        if next_page_url:
            current_url = next_page_url
            params = {}  # Clear params since the next URL contains the query params
        else:
            more_data = False  # End pagination if there is no 'next' URL in the response.


# The get_api_response function sends an HTTP GET request to the provided URL with the specified parameters.
# It performs the following tasks:
# 1. Logs the URL and query parameters used for the API call for debugging and tracking purposes.
# 2. Makes the API request using the 'requests' library, passing the URL and parameters.
# 3. Parses the JSON response from the API and returns it as a dictionary.
#
# The function takes two parameters:
# - current_url: The URL to which the API request is made.
# - params: A dictionary of query parameters to be included in the API request.
#
# Returns:
# - response_page: A dictionary containing the parsed JSON response from the API.
#
# Api response looks like:
# {
#   "pages": {
#     "type": "pages",
#     "next": {
#       "page": "https://example.com/api/items/paginate_using_next_page_url?order_by=updatedAt
#                                   &order=asc&since=0001-01-01T00:00:00Z&per_page=100&page=2"
#     },
#     "page": 1,
#     "per_page": 100,
#     "total_pages": 50
#   }
#   ...
# }
#
# For real API example you can refer Drift's Account Listing API: https://devdocs.drift.com/docs/listing-accounts
def get_api_response(current_url, params):
    log.info(f"Making API call to url: {current_url} with params: {params}")
    response = rq.get(current_url, params=params)
    response_page = response.json()
    return response_page


# The get_next_page_url_from_response function extracts the URL for the next page of data from the API response.
#
# The function takes one parameter:
# - response_page: A dictionary representing the parsed JSON response from the API.
#
# Returns:
# - The URL for the next page if it exists, otherwise None.

def get_next_page_url_from_response(response_page):
    return response_page.get("pages", {}).get("next", {}).get("page")  # TODO: UPDATE AS PER YOUR API RESPONSE


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
